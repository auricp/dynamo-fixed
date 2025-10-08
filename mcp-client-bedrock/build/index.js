import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import dotenv from "dotenv";
// Load environment variables
dotenv.config({ path: '.env' });
const AWS_REGION = 'us-east-1';
if (!AWS_REGION) {
    throw new Error("AWS_REGION is not set in environment variables");
}
class EnhancedMCPClient {
    mcp;
    bedrockClient;
    transport = null;
    tools = [];
    modelId = "anthropic.claude-3-sonnet-20240229-v1:0";
    inferenceProfileId = null;
    sanitizedToOriginalToolName = {};
    constructor(inferenceProfileId) {
        this.bedrockClient = new BedrockRuntimeClient({ region: AWS_REGION });
        this.mcp = new Client({ name: "enhanced-mcp-client", version: "1.0.0" });
        this.inferenceProfileId = inferenceProfileId || null;
    }
    async connectToServer(serverScriptPath) {
        try {
            const isJs = serverScriptPath.endsWith(".js");
            const isPy = serverScriptPath.endsWith(".py");
            if (!isJs && !isPy) {
                throw new Error("Server script must be a .js or .py file");
            }
            const command = isPy
                ? process.platform === "win32" ? "python" : "python3"
                : process.execPath;
            this.transport = new StdioClientTransport({
                command,
                args: [serverScriptPath],
            });
            await this.mcp.connect(this.transport);
            const toolsResult = await this.mcp.listTools();
            this.tools = toolsResult.tools.map((tool) => ({
                name: tool.name,
                description: tool.description || "",
                input_schema: tool.inputSchema,
            }));
            // Build mapping from sanitized to original tool names
            this.sanitizedToOriginalToolName = {};
            for (const tool of this.tools) {
                this.sanitizedToOriginalToolName[this.sanitizeToolName(tool.name)] = tool.name;
            }
            console.log(`Connected to MCP Server with ${this.tools.length} tools`);
        }
        catch (e) {
            console.error("Failed to connect to MCP server:", e);
            throw e;
        }
    }
    // Enhanced query optimization - smarter tool selection
    optimizeQuery(toolName, toolArgs) {
        if (toolName === "dynamodb:query_table") {
            const keyCondition = toolArgs.keyConditionExpression || "";
            if (keyCondition.includes("Age") && !keyCondition.includes("Name") && !keyCondition.includes("#name")) {
                return {
                    name: "dynamodb:scan_table",
                    args: {
                        tableName: toolArgs.tableName,
                        filterExpression: toolArgs.keyConditionExpression,
                        expressionAttributeNames: toolArgs.expressionAttributeNames,
                        expressionAttributeValues: toolArgs.expressionAttributeValues,
                        limit: toolArgs.limit,
                    }
                };
            }
        }
        return { name: toolName, args: toolArgs };
    }
    // Utility to sanitize tool names for Bedrock
    sanitizeToolName(name) {
        return name.replace(/[^a-zA-Z0-9_-]/g, '_');
    }
    async processQuery(query, stateless = true) {
        const userMessage = {
            role: "user",
            content: [{ type: "text", text: query }]
        };
        // Prepare tools for Bedrock format
        const toolsForBedrock = this.tools.length > 0 ? {
            tools: this.tools.map(tool => ({
                name: this.sanitizeToolName(tool.name),
                description: tool.description || "",
                input_schema: tool.input_schema
            }))
        } : {};
        // Create the request payload
        const payload = {
            anthropic_version: "bedrock-2023-05-31",
            max_tokens: 2000,
            top_k: 250,
            stop_sequences: [],
            temperature: 0.1,
            top_p: 0.999,
            messages: [userMessage],
            ...toolsForBedrock
        };
        const commandParams = {
            modelId: this.modelId,
            contentType: "application/json",
            accept: "application/json",
            body: JSON.stringify(payload)
        };
        if (this.inferenceProfileId) {
            commandParams.inferenceProfileArn = this.inferenceProfileId;
        }
        const command = new InvokeModelCommand(commandParams);
        try {
            const response = await this.bedrockClient.send(command);
            const responseBody = JSON.parse(new TextDecoder().decode(response.body));
            let finalText = [];
            let toolResults = [];
            for (const content of responseBody.content) {
                if (content.type === "text") {
                    finalText.push(content.text);
                }
                else if (content.type === "tool_use") {
                    // Execute the tool
                    const optimized = this.optimizeQuery(content.name, content.input);
                    const sanitizedToolName = this.sanitizeToolName(optimized.name);
                    const mcpToolName = this.sanitizedToOriginalToolName[sanitizedToolName] || optimized.name;
                    let result;
                    try {
                        result = await this.mcp.callTool({
                            name: mcpToolName,
                            arguments: optimized.args,
                        });
                    }
                    catch (err) {
                        return `❌ Tool ${sanitizedToolName} failed: ${err}`;
                    }
                    // Parse tool result
                    let parsedResult = null;
                    let resultText = "";
                    try {
                        if (typeof result.content === "string") {
                            parsedResult = JSON.parse(result.content);
                        }
                        else if (Array.isArray(result.content) && result.content.length > 0 && typeof result.content[0]?.text === "string") {
                            parsedResult = JSON.parse(result.content[0].text);
                        }
                    }
                    catch (parseError) { }
                    if (parsedResult) {
                        resultText = JSON.stringify(parsedResult, null, 2);
                        toolResults.push(parsedResult);
                    }
                    // Send follow-up request with tool result
                    const followUpMessages = [
                        userMessage,
                        { role: "assistant", content: [content] },
                        { role: "user", content: [{ type: "tool_result", tool_use_id: content.id, content: resultText }] }
                    ];
                    const followUpPayload = {
                        anthropic_version: "bedrock-2023-05-31",
                        max_tokens: 1000,
                        top_k: 250,
                        stop_sequences: [],
                        temperature: 0.1,
                        top_p: 0.999,
                        messages: followUpMessages,
                        tools: this.tools.map(tool => ({
                            name: this.sanitizeToolName(tool.name),
                            description: tool.description || "",
                            input_schema: tool.input_schema
                        }))
                    };
                    const followUpCommandParams = {
                        modelId: this.modelId,
                        contentType: "application/json",
                        accept: "application/json",
                        body: JSON.stringify(followUpPayload)
                    };
                    if (this.inferenceProfileId) {
                        followUpCommandParams.inferenceProfileArn = this.inferenceProfileId;
                    }
                    const followUpCommand = new InvokeModelCommand(followUpCommandParams);
                    const followUpResponse = await this.bedrockClient.send(followUpCommand);
                    const followUpBody = JSON.parse(new TextDecoder().decode(followUpResponse.body));
                    for (const followContent of followUpBody.content) {
                        if (followContent.type === "text") {
                            finalText.push(followContent.text);
                        }
                    }
                }
            }
            // Return structured data if tool results exist
            if (toolResults.length > 0) {
                const toolResult = toolResults[0];
                if (toolResult.success && toolResult.items) {
                    return JSON.stringify({
                        success: true,
                        count: toolResult.items.length,
                        items: toolResult.items,
                        message: toolResult.message || "Query executed successfully"
                    });
                }
                if (toolResult.success) {
                    return JSON.stringify(toolResult);
                }
                return JSON.stringify({
                    success: false,
                    error: toolResult.message || "Unknown error",
                    errorType: toolResult.errorType
                });
            }
            return finalText.join("\n");
        }
        catch (error) {
            console.error("Error invoking Bedrock model:", error);
            return `Error: ${error.message || error}`;
        }
    }
    async cleanup() {
        try {
            await this.mcp.close();
        }
        catch (error) {
            console.error("Error closing MCP connection:", error);
        }
    }
}
let mcpClientInstance = null;
export async function initMCPClient(serverScriptPath, inferenceProfileId) {
    if (!mcpClientInstance) {
        mcpClientInstance = new EnhancedMCPClient(inferenceProfileId);
        await mcpClientInstance.connectToServer(serverScriptPath);
    }
    return mcpClientInstance;
}
export async function mcpProcessQuery(query, stateless = true) {
    if (!mcpClientInstance) {
        throw new Error("MCP client not initialized. Call initMCPClient first.");
    }
    return await mcpClientInstance.processQuery(query, stateless);
}
