#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import {
  DynamoDBClient,
  ListTablesCommand,
  DescribeTableCommand,
  GetItemCommand,
  QueryCommand,
  ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

// AWS client initialization

const accessKeyId = '';
const secretAccessKey = '';

const dynamoClient = new DynamoDBClient({
  region: 'us-east-1',
  ...(accessKeyId && secretAccessKey
    ? { credentials: { accessKeyId, secretAccessKey } }
    : {}),
});

// Read-only tool definitions
const DYNAMODB_LIST_TABLES_TOOL: Tool = {
  name: "dynamodb:list_tables",
  description: "Lists all DynamoDB tables in the account",
  inputSchema: {
    type: "object",
    properties: {
      limit: { type: "number", description: "Maximum number of tables to return (optional)" },
      exclusiveStartTableName: { type: "string", description: "Name of the table to start from for pagination (optional)" },
    },
  },
};

const DYNAMODB_DESCRIBE_TABLE_TOOL: Tool = {
  name: "dynamodb:describe_table",
  description: "Gets detailed information about a DynamoDB table including schema, indexes, and capacity",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table to describe" },
    },
    required: ["tableName"],
  },
};

const DYNAMODB_GET_ITEM_TOOL: Tool = {
  name: "dynamodb:get_item",
  description: "Retrieves an item from a table by its primary key",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      key: { type: "object", description: "Primary key of the item to retrieve" },
    },
    required: ["tableName", "key"],
  },
};

const DYNAMODB_QUERY_TABLE_TOOL: Tool = {
  name: "dynamodb:query_table",
  description: "Queries a table using key conditions and optional filters. Most efficient for retrieving items with known partition key.",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      keyConditionExpression: { type: "string", description: "Key condition expression (required for query)" },
      expressionAttributeValues: { type: "object", description: "Values for the key condition expression" },
      expressionAttributeNames: { type: "object", description: "Attribute name mappings (optional)" },
      filterExpression: { type: "string", description: "Filter expression for results (optional)" },
      limit: { type: "number", description: "Maximum number of items to return (optional)" },
      indexName: { type: "string", description: "Name of the index to query (optional)" },
      scanIndexForward: { type: "boolean", description: "Sort order for range key (true=ascending, false=descending)" },
    },
    required: ["tableName", "keyConditionExpression", "expressionAttributeValues"],
  },
};

const DYNAMODB_SCAN_TABLE_TOOL: Tool = {
  name: "dynamodb:scan_table",
  description: "Scans an entire table with optional filters. Use for full table scans or when partition key is unknown.",
  inputSchema: {
    type: "object", 
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      filterExpression: { type: "string", description: "Filter expression (optional)" },
      expressionAttributeValues: { type: "object", description: "Values for the filter expression (optional)" },
      expressionAttributeNames: { type: "object", description: "Attribute name mappings (optional)" },
      limit: { type: "number", description: "Maximum number of items to return (optional)" },
      indexName: { type: "string", description: "Name of the index to scan (optional)" },
    },
    required: ["tableName"],
  },
};

// Utility functions
function normalizeAttributeValues(exprAttrVals: any): any {
  if (!exprAttrVals) return undefined;
  
  if (typeof exprAttrVals === "string") {
    try {
      exprAttrVals = JSON.parse(exprAttrVals);
    } catch (err) {
      console.error("Error parsing expressionAttributeValues:", err);
      return undefined;
    }
  }

  const normalized: any = {};
  Object.keys(exprAttrVals).forEach(key => {
    const val = exprAttrVals[key];
    if (val && typeof val === "object" && (val.N || val.S || val.B)) {
      if (val.N !== undefined) normalized[key] = Number(val.N);
      else if (val.S !== undefined) normalized[key] = val.S;
      else if (val.B !== undefined) normalized[key] = val.B;
    } else {
      normalized[key] = val;
    }
  });
  
  return normalized;
}

function cleanExpressionAttributeNames(expressionAttributeNames: any, expressions: string[]): any {
  if (!expressionAttributeNames) return undefined;
  
  const combinedExpression = expressions.filter(e => typeof e === "string").join(" ");
  const cleanedNames: any = {};
  
  Object.keys(expressionAttributeNames).forEach(key => {
    if (key === "#") {
      throw new Error('Invalid ExpressionAttributeNames key: "#" is not allowed. Use descriptive names like "#age", "#name".');
    }
    if (combinedExpression.includes(key)) {
      cleanedNames[key] = expressionAttributeNames[key];
    }
  });
  
  return Object.keys(cleanedNames).length > 0 ? cleanedNames : undefined;
}

async function getTableSchema(tableName: string) {
  try {
    const descCmd = new DescribeTableCommand({ TableName: tableName });
    const descResp = await dynamoClient.send(descCmd);
    return descResp.Table;
  } catch (error) {
    console.error(`Error getting table schema for ${tableName}:`, error);
    return null;
  }
}

function fixItemKeyTypes(item: any, tableSchema: any): any {
  if (!tableSchema?.KeySchema || !tableSchema?.AttributeDefinitions || !item) {
    return item;
  }

  const keyAttrs = tableSchema.KeySchema
    .map((k: any) => k.AttributeName)
    .filter((attr: any): attr is string => typeof attr === "string");
    
  const attrTypes: Record<string, string> = {};
  tableSchema.AttributeDefinitions.forEach((def: any) => {
    if (typeof def.AttributeName === "string" && typeof def.AttributeType === "string") {
      attrTypes[def.AttributeName] = def.AttributeType;
    }
  });

  const fixedItem = { ...item };
  keyAttrs.forEach((attr: string | number) => {
    if (fixedItem[attr] !== undefined) {
      const expectedType = attrTypes[attr];
      if (expectedType === "S" && typeof fixedItem[attr] !== "string") {
        fixedItem[attr] = typeof fixedItem[attr] === "object" 
          ? JSON.stringify(fixedItem[attr])
          : String(fixedItem[attr]);
      }
      if (expectedType === "N" && typeof fixedItem[attr] !== "number") {
        const num = Number(fixedItem[attr]);
        if (!isNaN(num)) fixedItem[attr] = num;
      }
    }
  });

  return fixedItem;
}

// Read-only implementation functions
async function listTables(params: any = {}) {
  try {
    const command = new ListTablesCommand({
      Limit: params.limit,
      ExclusiveStartTableName: params.exclusiveStartTableName,
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: "Tables listed successfully",
      tables: response.TableNames || [],
      lastEvaluatedTable: response.LastEvaluatedTableName,
      tableCount: response.TableNames?.length || 0,
    };
  } catch (error: any) {
    console.error("Error listing tables:", error);
    return {
      success: false,
      message: `Failed to list tables: ${error.message || error}`,
    };
  }
}

async function describeTable(params: any) {
  try {
    const command = new DescribeTableCommand({
      TableName: params.tableName,
    });
    
    const response = await dynamoClient.send(command);
    const table = response.Table;
    
    return {
      success: true,
      message: `Table ${params.tableName} described successfully`,
      table: table,
      summary: {
        tableName: table?.TableName,
        status: table?.TableStatus,
        itemCount: table?.ItemCount,
        tableSize: table?.TableSizeBytes,
        partitionKey: table?.KeySchema?.find(k => k.KeyType === "HASH")?.AttributeName,
        sortKey: table?.KeySchema?.find(k => k.KeyType === "RANGE")?.AttributeName,
        gsiCount: table?.GlobalSecondaryIndexes?.length || 0,
        lsiCount: table?.LocalSecondaryIndexes?.length || 0,
      }
    };
  } catch (error: any) {
    console.error("Error describing table:", error);
    return {
      success: false,
      message: `Failed to describe table: ${error.message || error}`,
    };
  }
}

async function getItem(params: any) {
  try {
    const tableSchema = await getTableSchema(params.tableName);
    const fixedKey = fixItemKeyTypes(params.key, tableSchema);

    const command = new GetItemCommand({
      TableName: params.tableName,
      Key: marshall(fixedKey),
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: response.Item 
        ? `Item retrieved successfully from table ${params.tableName}`
        : `No item found with the specified key in table ${params.tableName}`,
      item: response.Item ? unmarshall(response.Item) : null,
      found: !!response.Item,
    };
  } catch (error: any) {
    console.error("Error getting item:", error);
    return {
      success: false,
      message: `Failed to get item: ${error.message || error}`,
    };
  }
}

async function queryTable(params: any) {
  try {
    const scanParams: any = {
      tableName: params.tableName,
      indexName: params.indexName,
      filterExpression: params.keyConditionExpression,
      expressionAttributeNames: params.expressionAttributeNames,
      expressionAttributeValues: params.expressionAttributeValues,
      limit: params.limit,
    };
    const scanResult = await scanTable(scanParams);
    scanResult.message = `Query converted to scan: ${scanResult.message}`;
    return scanResult;
    } catch (scanError) {
      console.error("Scan fallback also failed:", scanError);
      return {
        success: false,
        message: `Query failed and scan fallback also failed: ${scanError}`,
        items: [],
    };
  };
}


async function scanTable(params: any) {
  try {
    const normalizedValues = normalizeAttributeValues(params.expressionAttributeValues);
    const cleanedNames = cleanExpressionAttributeNames(
      params.expressionAttributeNames,
      [params.filterExpression].filter(Boolean)
    );

    const command = new ScanCommand({
      TableName: params.tableName,
      IndexName: params.indexName,
      FilterExpression: params.filterExpression,
      ExpressionAttributeValues: normalizedValues ? marshall(normalizedValues) : undefined,
      ExpressionAttributeNames: cleanedNames,
      Limit: params.limit,
    });
    
    const response = await dynamoClient.send(command);
    const items = response.Items ? response.Items.map(item => unmarshall(item)) : [];
    
    return {
      success: true,
      message: `Scan executed successfully on table ${params.tableName}${params.indexName ? ` (index: ${params.indexName})` : ''}`,
      items: items,
      count: response.Count,
      scannedCount: response.ScannedCount,
      lastEvaluatedKey: response.LastEvaluatedKey ? unmarshall(response.LastEvaluatedKey) : null,
      consumedCapacity: response.ConsumedCapacity,
    };
  } catch (error: any) {
    console.error("Error scanning table:", error);
    return {
      success: false,
      message: `Failed to scan table: ${error.message || error}`,
      items: [],
      errorType: error.name,
    };
  }
}

// Server setup
const server = new Server(
  {
    name: "dynamodb-mcp-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

// Only read-only tools
const READ_ONLY_TOOLS = [
  DYNAMODB_LIST_TABLES_TOOL,
  DYNAMODB_DESCRIBE_TABLE_TOOL,
  DYNAMODB_GET_ITEM_TOOL,
  DYNAMODB_QUERY_TABLE_TOOL,
  DYNAMODB_SCAN_TABLE_TOOL,
];

// Request handlers
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: READ_ONLY_TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result;
    
    // Only read operations
    switch (name) {
      case "dynamodb:list_tables":
        result = await listTables(args);
        break;
      case "dynamodb:describe_table":
        result = await describeTable(args);
        break;
      case "dynamodb:get_item":
        result = await getItem(args);
        break;
      case "dynamodb:query_table":
        result = await queryTable(args);
        break;
      case "dynamodb:scan_table":
        result = await scanTable(args);
        break;
      default:
        return {
          content: [{ 
            type: "text", 
            text: JSON.stringify({
              success: false,
              message: `Unknown tool: ${name}`,
              availableTools: READ_ONLY_TOOLS.map(t => t.name)
            }, null, 2)
          }],
          isError: true,
        };
    }

    const responseText = JSON.stringify(result, null, 2);
    
    return {
      content: [{ type: "text", text: responseText }],
      isError: !result?.success,
    };
    
  } catch (error: any) {
    console.error(`Error executing tool ${name}:`, error);
    
    const errorResponse = {
      success: false,
      message: `Unexpected error occurred while executing ${name}: ${error.message || error}`,
      errorType: error.name || "UnknownError",
      tool: name,
      arguments: args,
    };
    
    return {
      content: [{ type: "text", text: JSON.stringify(errorResponse, null, 2) }],
      isError: true,
    };
  }
});

// Enhanced server startup with better error handling
async function runServer() {
  try {
    const transport = new StdioServerTransport();
    await server.connect(transport);
    
    // Log startup info to stderr so it doesn't interfere with MCP communication
    console.error("=".repeat(50));
    console.error("DynamoDB MCP Server v1.0.0 (Read-Only)");
    console.error("=".repeat(50));
    console.error("Server running on stdio transport");
    console.error(`AWS Region: ${process.env.AWS_REGION || 'not set'}`);
    console.error(`Available tools: ${READ_ONLY_TOOLS.length}`);
    console.error("Tools:", READ_ONLY_TOOLS.map(t => `  - ${t.name}`).join('\n'));
    console.error("=".repeat(50));
    
  } catch (error) {
    console.error("Fatal error starting server:", error);
    process.exit(1);
  }
}

// Enhanced error handling and graceful shutdown
process.on('SIGINT', () => {
  console.error("Received SIGINT, shutting down gracefully...");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.error("Received SIGTERM, shutting down gracefully...");
  process.exit(0);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

// Start the server
runServer().catch((error) => {
  console.error("Fatal error running server:", error);
  process.exit(1);
});