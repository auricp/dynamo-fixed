
// Helper to map natural language keywords to best matching attribute in the schema
function findBestAttributeMatch(queryText: string, attributeNames: string[]): string | undefined {
  const lowerQuery = queryText.toLowerCase();

  // 1. Direct match (e.g., query includes 'Amount', attribute is 'Amount')
  for (const attr of attributeNames) {
    if (lowerQuery.includes(attr.toLowerCase())) {
      return attr; // Return the exact attribute name from the schema
    }
  }

  // 2. Keyword-to-Attribute mapping (Heuristics for common schema patterns)
  // Maps common natural language concepts to likely column names.
  const keywordMap: [string | RegExp, string[]][] = [
    // Maps "from" or "source" to attributes that contain 'Source' or 'From'
    [/\b(from|source)\b/i, ['SourceCountry', 'source', 'from']],
    // Maps "to" or "destination" to attributes that contain 'Destination' or 'To'
    [/\b(to|destination)\b/i, ['DestinationCountry', 'destination', 'to']],
    // Maps monetary concepts to attributes like 'Amount' or 'Price'
    [/\b(amt|cost|price|value)\b/i, ['Amount', 'amount', 'cost', 'price', 'value']],
    // Maps temporal concepts to attributes like 'Date'
    [/\b(when|day|time|period)\b/i, ['Date', 'date', 'timestamp']],
    // Maps grouping concepts to attributes like 'Type'
    [/\b(type|category)\b/i, ['TradeType', 'type', 'category']],
  ];

  for (const [keyword, potentialMatches] of keywordMap) {
    if (lowerQuery.match(keyword)) {
      for (const match of potentialMatches) {
        // Find the schema attribute that contains the potential match name
        const schemaMatch = attributeNames.find(a => a.toLowerCase().includes(match.toLowerCase()));
        if (schemaMatch) {
          return schemaMatch;
        }
      }
    }
  }

  return undefined; // No match found
}




export async function smartQuery(params: any) {
  const tableName = params.tableName;
  const queryText = params.queryText;

  if (!tableName || !queryText) {
    return {
      success: false,
      items: [],
      message: "tableName and queryText are required",
      count: 0,
      errorType: "InvalidArgument"
    };
  }

  // 1. Dynamically get table schema
  const tableSchema = await getTableSchema(tableName);
  if (!tableSchema || !tableSchema.AttributeDefinitions) {
      return { success: false, items: [], message: `Could not retrieve schema for table ${tableName}.`, count: 0, errorType: "SchemaError" };
  }

  // Get a list of all potential attributes from the schema and item keys (if available)
  const attributeNames = (tableSchema.AttributeDefinitions.map((def: any) => def.AttributeName) as string[]).filter(name => name);

  const lowerQuery = queryText.toLowerCase();

  let filterExpression: string | undefined;
  let expressionAttributeNames: Record<string, string> = {};
  let expressionAttributeValues: Record<string, any> = {};
  let filterParts: string[] = []; // To accumulate filter parts

  try {
    // --------------------------
    // A. Handle highest/lowest queries (Advanced sorting)
    // --------------------------
    const numericFields = attributeNames.filter(name => name.toLowerCase().includes('amount') || name.toLowerCase().includes('value'));
    const numericField = numericFields.length > 0 ? numericFields[0] : null; // Use the first found numeric field

    const isHighest = lowerQuery.includes("highest") || lowerQuery.includes("maximum") || lowerQuery.includes("max");
    const isLowest = lowerQuery.includes("lowest") || lowerQuery.includes("minimum") || lowerQuery.includes("min");

    if ((isHighest || isLowest) && numericField) {
      // Perform a full scan to get all items and sort them in-memory
      const scanCmd = new ScanCommand({ TableName: tableName });
      const result = await dynamoClient.send(scanCmd);
      
      // Reverted to implicit 'any[]'
      const items = (result.Items ?? []).map(item => unmarshall(item));

      // Reverted sort function parameter types to 'any'
      items.sort((a: any, b: any) =>
        isHighest
          ? (Number(b[numericField] ?? 0) || 0) - (Number(a[numericField] ?? 0) || 0)
          : (Number(a[numericField] ?? 0) || 0) - (Number(b[numericField] ?? 0) || 0)
      );

      return {
        success: true,
        items: items.slice(0, params.limit || 1),
        message: `SmartQuery executed: returning ${isHighest ? "highest" : "lowest"} ${numericField} item(s) by scanning the table.`,
        count: items.slice(0, params.limit || 1).length,
      };
    }

    // --------------------------
    // B. Handle Filter Expressions (New Solution: Attribute-Centric Parsing)
    // --------------------------
    let valueIndex = 0;
    // Regex to detect common date formats (YYYY-MM-DD or variations)
    const datePattern = /\d{4}[-/]\d{2}[-/]\d{2}/;
    let filterFound = false;

    // Helper function to process, add a filter part, and increment valueIndex
    const processAndAddFilter = (targetAttribute: string, op: string, rawValue: string) => {
        let value = rawValue.trim().replace(/['"]/g, '');
        
        if (!value) return false; // Return false if no value found

        const key = `:val${valueIndex}`;
        const hashName = `#attr${valueIndex}`;
        
        let parsedValue: any;
        const isDateField = targetAttribute.toLowerCase().includes('date') || targetAttribute.toLowerCase().includes('time');

        if (isDateField && value.match(datePattern)) {
            // Robust Date Parsing for range comparisons
            const date = new Date(value);
            if (!isNaN(date.getTime())) {
                // Normalize to YYYY-MM-DD for consistent string comparison.
                // This ensures 'after 2025-02-25' correctly maps to Date > '2025-02-25'
                parsedValue = date.toISOString().split('T')[0];
            } else {
                parsedValue = value; // Fallback to raw string
            }
        } else {
            // Handle numbers and other strings
            parsedValue = !isNaN(parseFloat(value)) && isFinite(parseFloat(value)) ? parseFloat(value) : value;
        }

        // Add the filter condition
        filterParts.push(`${hashName} ${op} ${key}`);

        // Populate expression attribute names and values
        expressionAttributeValues[key] = parsedValue;
        expressionAttributeNames[hashName] = targetAttribute;
        valueIndex++;
        return true; // Filter successfully added
    };

    // 1. Try to find the implied target attribute first using keywords (e.g., 'date', 'source')
    const impliedTargetAttribute = findBestAttributeMatch(queryText, attributeNames);

    if (impliedTargetAttribute) {
        // A. Look for Natural Language Operators + Value (e.g., 'after 2025-02-25', 'less than 100')
        const naturalOpMatch = queryText.match(/(after|before|less than|greater than)\s+([A-Za-z0-9\s.-]+)/i);

        if (naturalOpMatch) {
            const operatorPhrase = naturalOpMatch[1].toLowerCase();
            let op = '=';
            if (operatorPhrase.includes('after') || operatorPhrase.includes('greater than')) op = '>';
            else if (operatorPhrase.includes('before') || operatorPhrase.includes('less than')) op = '<';
            
            if (processAndAddFilter(impliedTargetAttribute, op, naturalOpMatch[2])) {
                 filterFound = true;
            }
        }
    }

    // 2. Look for Explicit Attribute + Operator + Value (e.g., 'Amount > 100' or 'Date <= 2025-01-01')
    // This pattern handles structured queries well.
    if (!filterFound) {
        // The regex captures a keyword/attribute, an operator, and a value
        const explicitMatch = queryText.match(/(\w+)\s*(>=|<=|>|<|=)\s*([A-Za-z0-9\s.-]+)/i);
        
        if (explicitMatch) {
            // explicitMatch[1] = Attribute Name (or keyword), explicitMatch[2] = Operator, explicitMatch[3] = Value
            // Use findBestAttributeMatch on the captured attribute/keyword to map it back to a schema name
            const matchedAttr = findBestAttributeMatch(explicitMatch[1], attributeNames); 
            if (matchedAttr) {
                if (processAndAddFilter(matchedAttr, explicitMatch[2], explicitMatch[3])) {
                    filterFound = true;
                }
            }
        }
    }
    
    // 3. Fallback: Simple Attribute + Value (implied equality) (e.g., 'country China', 'type domestic')
    if (!filterFound) {
        const simpleMatch = queryText.match(/(\w+)\s+([A-Za-z0-9\s-]+)/i);
        if (simpleMatch) {
            // simpleMatch[1] = Attribute/Keyword, simpleMatch[2] = Value
            const matchedAttr = findBestAttributeMatch(simpleMatch[1], attributeNames);
            if (matchedAttr) {
                if (processAndAddFilter(matchedAttr, '=', simpleMatch[2])) {
                    filterFound = true;
                }
            }
        }
    }
    
    if (filterParts.length > 0) {
        filterExpression = filterParts.join(" AND ");
    }


    // --------------------------
    // C. Final Scan execution
    // --------------------------
    if (filterExpression) {
        // Convert values to DynamoDB format
        const marshalledValues = Object.keys(expressionAttributeValues).length > 0
            ? marshall(expressionAttributeValues)
            : undefined;

        const scanCmd = new ScanCommand({
            TableName: tableName,
            FilterExpression: filterExpression,
            ExpressionAttributeNames:
                Object.keys(expressionAttributeNames).length > 0 ? expressionAttributeNames : undefined,
            ExpressionAttributeValues: marshalledValues,
            Limit: params.limit,
        });

        const result = await dynamoClient.send(scanCmd);
        // Reverted to implicit 'any[]'
        const items = (result.Items ?? []).map(item => unmarshall(item));

        return {
            success: true,
            items,
            message: `SmartQuery executed successfully on table ${tableName}. Filter: ${filterExpression}`,
            count: items.length,
        };
    } else {
         // If no filter expression could be generated, but it wasn't a highest/lowest request, 
         // we assume it's an unsupported query or missing a primary key.
         return {
            success: false,
            items: [],
            message: "SmartQuery could not generate a valid filter expression from the natural language query. Please be more specific or use the exact attribute name.",
            count: 0,
            errorType: "InvalidFilter"
        };
    }
  } catch (err: any) {
    console.error("SmartQuery Execution Error:", err);
    return {
      success: false,
      items: [],
      message: `SmartQuery failed: ${err.message || err}`,
      count: 0,
      errorType: "InternalError"
    };
  }
}


export const DYNAMODB_SMART_QUERY_TOOL: Tool = {
  name: "dynamodb:smart_query",
  description: 
    "**FALLBACK TOOL:** Performs an advanced, interpretive query on a DynamoDB table. Use this ONLY as a last resort when attempts with structured query tools (like 'dynamodb:query' or 'dynamodb:scan') have failed, or if the user's request is highly generalized (e.g., 'get the highest amount') or requires complex natural language interpretation. Requires tableName and queryText arguments.",
  inputSchema: {
    type: "object",
    properties: {
      tableName: {
        type: "string",
        description: "The name of the DynamoDB table to query.",
      },
      queryText: {
        type: "string",
        description: "The natural language query provided by the user (e.g., 'records from China', 'highest Amount').",
      },
      limit: {
        type: "number",
        description: "Optional: The maximum number of items to return (default is 100).",
      },
    },
    required: ["tableName", "queryText"],
  },
};
