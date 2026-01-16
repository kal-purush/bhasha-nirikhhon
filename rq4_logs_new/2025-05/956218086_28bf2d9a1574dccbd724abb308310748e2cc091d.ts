import { SkillEngine } from '../src/engine';
import { createAndStartReflyMcpServer, LogLevel } from '../src/mcp';
import { User } from '@refly/openapi-schema';

/**
 * This script starts the Refly MCP server for testing purposes
 */
async function startMcpServer() {
  console.log('Starting Refly MCP server...');

  // Create a mock logger
  const mockLogger = {
    error: console.error,
    log: console.log,
    warn: console.warn,
    debug: console.debug,
  };

  // Create a default user
  const defaultUser = {
    uid: 'test-user',
    email: 'test@example.com',
  };

  try {
    // 创建一个简化的 ReflyService 模拟实现
    // 使用 any 类型来绕过类型检查
    const mockReflyService: any = {
      // Canvas 相关方法
      listCanvases: async (user: User, param: any) => {
        console.log('调用 listCanvases', user, param);
        return {
          success: true,
          data: [
            {
              id: 'canvas-1',
              canvasId: 'canvas-1',
              title: '测试画布 1',
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            },
            {
              id: 'canvas-2',
              canvasId: 'canvas-2',
              title: '测试画布 2',
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            },
          ],
        };
      },
      createCanvas: async (user: User, req: any) => {
        console.log('调用 createCanvas', user, req);
        return {
          success: true,
          data: {
            id: 'new-canvas-id',
            canvasId: 'new-canvas-id',
            title: req.title || '新画布',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        };
      },
      deleteCanvas: async (user: User, req: any) => {
        console.log('调用 deleteCanvas', user, req);
        return {
          success: true,
          data: { id: req.canvasId },
        };
      },

      // 文档相关方法
      listDocuments: async (user: User, param: any) => {
        console.log('调用 listDocuments', user, param);
        return {
          success: true,
          data: [
            {
              id: 'doc-1',
              docId: 'doc-1',
              title: '测试文档 1',
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            },
            {
              id: 'doc-2',
              docId: 'doc-2',
              title: '测试文档 2',
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
            },
          ],
        };
      },
      createDocument: async (user: User, req: any) => {
        console.log('调用 createDocument', user, req);
        return {
          success: true,
          data: {
            id: 'new-doc-id',
            docId: 'new-doc-id',
            title: req.title || '新文档',
            content: req.content || '这是一个新文档的内容',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        };
      },
      getDocumentDetail: async (user: User, req: any) => {
        console.log('调用 getDocumentDetail', user, req);
        return {
          success: true,
          data: {
            id: req.docId,
            docId: req.docId,
            title: '测试文档详情',
            content: '这是一个测试文档的内容。',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        };
      },
      deleteDocument: async (user: User, req: any) => {
        console.log('调用 deleteDocument', user, req);
        return {
          success: true,
          data: { id: req.docId },
        };
      },

      // 资源相关方法
      getResourceDetail: async (user: User, req: any) => {
        console.log('调用 getResourceDetail', user, req);
        return {
          success: true,
          data: {
            id: req.resourceId,
            resourceId: req.resourceId,
            title: '测试资源详情',
            content: '这是一个测试资源的内容。',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        };
      },
      createResource: async (user: User, req: any) => {
        console.log('调用 createResource', user, req);
        return {
          success: true,
          data: {
            id: 'new-resource-id',
            resourceId: 'new-resource-id',
            title: req.title || '新资源',
            content: req.content || '这是一个新资源的内容',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          },
        };
      },
      updateResource: async (user: User, req: any) => {
        console.log('调用 updateResource', user, req);
        return {
          success: true,
          data: {
            id: req.resourceId,
            resourceId: req.resourceId,
            title: req.title || '更新后的资源',
            content: req.content || '这是更新后的资源内容',
            updatedAt: new Date().toISOString(),
          },
        };
      },
      batchCreateResource: async (user: User, req: any) => {
        console.log('调用 batchCreateResource', user, req);
        return {
          success: true,
          data: req.map((item: any, index: number) => ({
            id: `new-resource-${index}`,
            resourceId: `new-resource-${index}`,
            title: item.title || `新资源 ${index}`,
            content: item.content || `这是新资源 ${index} 的内容`,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
          })),
        };
      },

      // 标签相关方法
      createLabelClass: async (user: User, req: any) => {
        console.log('调用 createLabelClass', user, req);
        return {
          success: true,
          data: {
            id: 'new-label-class-id',
            name: req.name || '新标签类',
            createdAt: new Date().toISOString(),
          },
        };
      },
      createLabelInstance: async (user: User, req: any) => {
        console.log('调用 createLabelInstance', user, req);
        return {
          success: true,
          data: {
            id: 'new-label-instance-id',
            classId: req.classId,
            name: req.name || '新标签实例',
            createdAt: new Date().toISOString(),
          },
        };
      },

      // 搜索相关方法
      search: async (user: User, req: any, options?: any) => {
        console.log('调用 search', user, req, options);
        return {
          success: true,
          data: [
            { id: 'result-1', title: '搜索结果 1', snippet: '这是搜索结果 1 的摘要' },
            { id: 'result-2', title: '搜索结果 2', snippet: '这是搜索结果 2 的摘要' },
          ],
        };
      },
      webSearch: async (user: User, req: any) => {
        console.log('调用 webSearch', user, req);
        return {
          success: true,
          data: [
            {
              title: '网页搜索结果 1',
              url: 'https://example.com/1',
              snippet: '这是网页搜索结果 1 的摘要',
            },
            {
              title: '网页搜索结果 2',
              url: 'https://example.com/2',
              snippet: '这是网页搜索结果 2 的摘要',
            },
          ],
        };
      },
      rerank: async (user: User, query: string, results: any[], options?: any) => {
        console.log('调用 rerank', user, query, results, options);
        return {
          success: true,
          data: results.map((result, index) => ({
            ...result,
            score: 1 - index * 0.1,
          })),
        };
      },

      // 引用相关方法
      addReferences: async (user: User, req: any) => {
        console.log('调用 addReferences', user, req);
        return {
          success: true,
          data: {
            added: req.references?.length || 0,
          },
        };
      },
      deleteReferences: async (user: User, req: any) => {
        console.log('调用 deleteReferences', user, req);
        return {
          success: true,
          data: {
            deleted: req.references?.length || 0,
          },
        };
      },

      // 内存搜索相关方法
      inMemorySearchWithIndexing: async (user: User, options: any) => {
        console.log('调用 inMemorySearchWithIndexing', user, options);
        return {
          success: true,
          data: {
            results: [
              { id: 'memory-1', title: '内存搜索结果 1', content: '这是内存搜索结果 1 的内容' },
              { id: 'memory-2', title: '内存搜索结果 2', content: '这是内存搜索结果 2 的内容' },
            ],
          },
        };
      },

      // URL 爬取方法
      crawlUrl: async (user: User, url: string) => {
        console.log('调用 crawlUrl', user, url);
        return {
          title: 'URL 页面标题',
          content: '这是从 URL 爬取的内容',
          metadata: {
            url,
            crawledAt: new Date().toISOString(),
          },
        };
      },

      // MCP 服务器相关方法
      listMcpServers: async (user: User, req: any) => {
        console.log('调用 listMcpServers', user, req);
        return {
          success: true,
          data: [
            { id: 'mcp-1', name: 'MCP 服务器 1', url: 'http://localhost:3001', enabled: true },
            { id: 'mcp-2', name: 'MCP 服务器 2', url: 'http://localhost:3002', enabled: true },
          ],
        };
      },
    };

    // 创建一个 SkillEngine 实例，使用模拟的 ReflyService
    const engine = new SkillEngine(mockLogger, mockReflyService);

    // Create and start the MCP server
    const server = await createAndStartReflyMcpServer(engine.service, defaultUser, {
      name: 'ReflyServiceMCP',
      version: '1.0.0',
      description: 'Refly Service MCP Server for testing',
      logLevel: LogLevel.DEBUG,
      port: 3000,
      cors: {
        origin: '*',
        methods: ['GET', 'POST'],
      },
    });

    console.log('Refly MCP server started successfully!');
    console.log('Server is running at http://localhost:3000/sse');
    console.log('You can test it using MCP Inspector:');
    console.log('npx @modelcontextprotocol/inspector http://localhost:3000/sse');

    // Keep the process running
    process.on('SIGINT', async () => {
      console.log('Stopping server...');
      await server.stop();
      process.exit(0);
    });

    console.log('Press Ctrl+C to stop the server');
  } catch (error) {
    console.error('Failed to start MCP server:', error);
  }
}

// Start the server
startMcpServer().catch(console.error);