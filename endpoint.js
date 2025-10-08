const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
app.use(bodyParser.json());

let mcpReady = false;
let mcpModule = null;

// Initialize MCP client once at startup
(async () => {
    try {
        // Use dynamic import for ES module
        mcpModule = await import('./mcp-client-bedrock/build/index.js');
        
        const serverScriptPath = path.resolve(__dirname, './mcp-dynamo/dist/index.js');
        const inferenceProfileId = process.env.INFERENCE_PROFILE_ID;
        await mcpModule.initMCPClient(serverScriptPath, inferenceProfileId);
        mcpReady = true;
        console.log("MCP client initialized.");
    } catch (err) {
        console.error("Failed to initialize MCP client:", err);
    }
})();

app.post('/query', async (req, res) => {
    if (!mcpReady || !mcpModule) {
        return res.status(503).json({ error: 'MCP client not ready.' });
    }
    const { query } = req.body;
    if (!query) {
        return res.status(400).json({ error: 'Query is required.' });
    }
    
    console.log('Processing query:', query); // Add query logging
    
    try {
        const result = await mcpModule.mcpProcessQuery(query, true); // stateless mode
        
        console.log('Raw MCP result:', result); // Log the raw result
        
        // Try to parse the result as JSON if it's a string
        let parsedResult;
        try {
            parsedResult = typeof result === 'string' ? JSON.parse(result) : result;
        } catch (e) {
            console.log('JSON parse failed, treating as text:', e.message);
            // If parsing fails, treat as text result
            parsedResult = { result };
        }
        
        console.log('Parsed result:', parsedResult); // Log parsed result
        
        // Check if the result indicates an error or failure
        if (parsedResult && parsedResult.success === false) {
            console.error('MCP operation failed:', parsedResult.message || parsedResult.error);
            return res.status(400).json({
                error: parsedResult.message || parsedResult.error || 'Operation failed',
                details: parsedResult,
                timestamp: new Date().toISOString()
            });
        }
        
        // If we have structured data with items, return it directly
        if (parsedResult && parsedResult.items) {
            res.json({
                success: parsedResult.success,
                count: parsedResult.count,
                items: parsedResult.items,
                message: parsedResult.message,
                timestamp: new Date().toISOString()
            });
        } else if (parsedResult && parsedResult.tables) {
            // Handle list tables response
            res.json({
                success: parsedResult.success,
                tables: parsedResult.tables,
                tableCount: parsedResult.tableCount,
                message: parsedResult.message,
                timestamp: new Date().toISOString()
            });
        } else if (parsedResult && parsedResult.table) {
            // Handle describe table response
            res.json({
                success: parsedResult.success,
                table: parsedResult.table,
                summary: parsedResult.summary,
                message: parsedResult.message,
                timestamp: new Date().toISOString()
            });
        } else if (parsedResult && typeof parsedResult === 'object' && parsedResult.success !== undefined) {
            // Return other structured results
            res.json({
                ...parsedResult,
                timestamp: new Date().toISOString()
            });
        } else {
            // Fallback to wrapping text result
            console.log('Fallback response for unstructured result');
            res.json({ 
                result: result,
                timestamp: new Date().toISOString()
            });
        }
    } catch (err) {
        console.error('Query error:', err);
        console.error('Error stack:', err.stack); // Add stack trace logging
        res.status(500).json({ 
            error: err.message,
            details: err.toString(),
            timestamp: new Date().toISOString()
        });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`MCP Endpoint listening on port ${PORT}`);
});
