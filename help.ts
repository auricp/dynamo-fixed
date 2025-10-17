
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
    // B. Handle Filter Expressions (Generalized Attribute/Value Mapping)
    // --------------------------
    let valueIndex = 0;

    // A simple regex to detect common date formats (YYYY-MM-DD or variations)
    const datePattern = /\d{4}[-/]\d{2}[-/]\d{2}/;

    // 1. Find simple string equality or range matches (e.g., 'from China', 'currency USD', 'amount > 100')
    const matchPatterns = [
        /(?:from|is|in|for)\s+([A-Za-z0-9\s-]+)/i, // e.g., 'from China', 'on 2025-02-25'
        /(\w+)\s*(>=|<=|>|<|=)\s*([A-Za-z0-9\s.-]+)/i, // e.g., 'Amount > 1000', 'Date > 2025-02-25'
        /(\w+)\s+([A-Za-z0-9\s-]+)/i // e.g., 'currency USD' (needs context)
    ];

    for (const pattern of matchPatterns) {
        let match;
        const tempQuery = queryText; 

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
                
                // --- FIX FOR DATE COMPARISON ---
                let parsedValue: any;
                const isDateField = targetAttribute.toLowerCase().includes('date') || targetAttribute.toLowerCase().includes('time');

                if (isDateField && value.match(datePattern)) {
                    // If it's a date field and the value looks like a date,
                    // parse it and convert it to a standard ISO string for consistent comparison.
                    const date = new Date(value);
                    if (!isNaN(date.getTime())) {
                        // Use the local date portion for comparison, adjusting for timezone if necessary
                        // This assumes dates are stored as YYYY-MM-DD or similar string format in DynamoDB
                        parsedValue = date.toISOString().split('T')[0]; // Use YYYY-MM-DD
                        
                        // If the operator is comparison (>, <, >=, <=) and it's a date, we need to adjust the value
                        // to encompass the entire day, but for basic string sorting, YYYY-MM-DD is often enough.
                        // If the stored dates include time, you may need to append 'T00:00:00.000Z' or 'T23:59:59.999Z'
                        // to the parsedValue based on the operator, but we'll stick to YYYY-MM-DD for simplicity.
                        // If the query includes time, the full date string will be used.
                    } else {
                        parsedValue = value; // Fallback to raw string if parsing fails
                    }
                } else {
                    // Handle numbers and other strings
                    parsedValue = !isNaN(parseFloat(value)) && isFinite(parseFloat(value)) ? parseFloat(value) : value;
                }
                // --- END FIX ---

                // Add the filter condition
                filterParts.push(`${hashName} ${op} ${key}`);

                // Populate expression attribute names and values
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
