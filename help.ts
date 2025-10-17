
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


// ----------------------------------------------------------------------
// 3. SMART QUERY TOOL FUNCTION (Generalized and Interpretive)
// ----------------------------------------------------------------------

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
      const items = (result.Items ?? []).map(item => unmarshall(item));

      items.sort((a, b) =>
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
    // B. Handle Filter Expressions (Generalized Attribute/Value Mapping)
    // --------------------------
    let valueIndex = 0;

    // 1. Find simple string equality or range matches (e.g., 'from China', 'currency USD', 'amount > 100')
    const matchPatterns = [
        /(?:from|is|in|for)\s+([A-Za-z0-9\s]+)/i, // e.g., 'from China'
        /(\w+)\s*(>=|<=|>|<|=)\s*([A-Za-z0-9\s.]+)/i, // e.g., 'Amount > 1000'
        /(\w+)\s+([A-Za-z0-9\s]+)/i // e.g., 'currency USD' (needs context)
    ];

    for (const pattern of matchPatterns) {
        let match;
        const tempQuery = queryText; // Use a temp variable for iterative regex matching if needed

        while ((match = pattern.exec(tempQuery)) !== null) {
            // Determine the value: it's either group 1, 3, or the final word of group 2 depending on the pattern
            let value = match[1] || match[3] || match[2] || '';
            value = value.trim().replace(/['"]/g, '');

            if (!value) continue;

            // Determine the field keyword based on the pattern
            let fieldKeyword = match.length > 3 ? match[1] : queryText.split(value)[0] || '';
            
            // Determine the operator
            let op = match[2] || '='; // For simple matches, assume '='

            // Use the helper to map the keyword/context to a schema attribute
            const targetAttribute = findBestAttributeMatch(queryText, attributeNames);

            if (targetAttribute) {
                const key = `:val${valueIndex}`;
                const hashName = `#attr${valueIndex}`;

                // Add the filter condition
                filterParts.push(`${hashName} ${op} ${key}`);

                // Populate expression attribute names and values, converting numbers if applicable
                const parsedValue = !isNaN(parseFloat(value)) && isFinite(parseFloat(value)) ? parseFloat(value) : value;
                expressionAttributeValues[key] = parsedValue;
                expressionAttributeNames[hashName] = targetAttribute;
                valueIndex++;
            }
            // Break after the first meaningful match for simplicity in this implementation
            break; 
        }
        if (filterParts.length > 0) break; // If a filter was created, stop trying other patterns
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


// ----------------------------------------------------------------------
// 4. TOOL DECLARATION (Updated Description)
// ----------------------------------------------------------------------

export const DYNAMODB_SMART_QUERY_TOOL: Tool = {
  name: "dynamodb:smart_query",
  description: 
    "Performs an advanced, interpretive query on a DynamoDB table. Use this ONLY when the user's request involves natural language filtering (e.g., 'records from China'), non-standard operators (e.g., 'get the highest amount'), or implicit attribute mapping (e.g., matching 'price' to an 'Amount' column). Requires tableName and queryText arguments.",
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
