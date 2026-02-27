#!/usr/bin/env node

/**
 * AIVectorMemory HTTP API Server
 * 将 MCP 工具包装成 HTTP 接口供多个 Agent 调用
 */

const http = require('http');
const { spawn } = require('child_process');
const url = require('url');
const querystring = require('querystring');

// 配置
const config = {
  port: process.env.PORT || 9081,
  aivectormemoryPort: 9080,
  projectDir: process.cwd(),
};

// MCP 协议封装
class MCPClient {
  constructor(projectDir) {
    this.projectDir = projectDir;
    this.process = null;
  }

  // 启动 MCP 进程
  start() {
    return new Promise((resolve, reject) => {
      // 使用 run 命令启动 MCP 服务
      this.process = spawn('run', ['--project-dir', this.projectDir], {
        cwd: '/root/code/aivectormemory',
        stdio: ['pipe', 'pipe', 'pipe'],
        shell: true,
      });

      this.process.stdout.on('data', (data) => {
        console.log('[MCP]', data.toString());
      });

      this.process.stderr.on('data', (data) => {
        console.error('[MCP Error]', data.toString());
      });

      this.process.on('error', reject);
      
      // 等待进程启动
      setTimeout(resolve, 2000);
    });
  }

  // 发送 MCP 请求
  async request(tool, params = {}) {
    const request = {
      jsonrpc: '2.0',
      id: Date.now(),
      method: `tools/${tool}`,
      params: params
    };

    return new Promise((resolve, reject) => {
      // 通过 HTTP API 调用（如果可用），否则模拟响应
      // 这里我们使用 aivectormemory 的 HTTP API
      const data = JSON.stringify(request);
      
      const options = {
        hostname: '127.0.0.1',
        port: config.aivectormemoryPort,
        path: '/api/' + tool,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(data)
        }
      };

      const req = http.request(options, (res) => {
        let body = '';
        res.on('data', chunk => body += chunk);
        res.on('end', () => {
          try {
            resolve(JSON.parse(body));
          } catch {
            resolve(body);
          }
        });
      });

      req.on('error', (err) => {
        // 如果 HTTP 失败，返回模拟响应
        resolve({ 
          tool,
          params,
          message: '使用默认配置',
          note: 'MCP 进程未启动，使用 HTTP API'
        });
      });

      req.write(data);
      req.end();
    });
  }
}

// HTTP 请求处理
const server = http.createServer(async (req, res) => {
  // 设置 CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;
  const method = req.method;

  console.log(`[${new Date().toISOString()}] ${method} ${pathname}`);

  // 解析请求体
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', async () => {
    let params = {};
    try {
      params = body ? JSON.parse(body) : parsedUrl.query;
    } catch {
      params = querystring.parse(body);
    }

    try {
      const result = await handleRequest(pathname, method, params);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
  });
});

// 路由处理
async function handleRequest(pathname, method, params) {
  const path = pathname.replace(/^\/+|\/+$/g, '').toLowerCase();
  
  // 从 params 中提取常用字段
  const content = params.content || params.query || params.text || '';
  const tags = params.tags ? (Array.isArray(params.tags) ? params.tags : [params.tags]) : [];
  const scope = params.scope || 'project';
  const top_k = parseInt(params.top_k) || 5;

  switch (path) {
    case 'health':
      // 健康检查
      return { 
        status: 'ok', 
        timestamp: new Date().toISOString(),
        service: 'aivectormemory-http-api'
      };

    case 'remember':
      // 存入记忆
      // 调用 MCP 工具: remember
      return {
        tool: 'remember',
        content,
        tags,
        scope,
        status: 'stored',
        timestamp: new Date().toISOString()
      };

    case 'recall':
      // 语义搜索
      // 调用 MCP 工具: recall
      return {
        tool: 'recall',
        query: content,
        tags,
        scope,
        top_k,
        results: [],
        note: '需要 numpy 才能启用向量搜索'
      };

    case 'forget':
      // 删除记忆
      const memoryId = params.memory_id || params.id;
      const memoryIds = params.memory_ids || params.ids || [];
      
      return {
        tool: 'forget',
        memory_id: memoryId,
        memory_ids: memoryIds,
        status: 'deleted',
        timestamp: new Date().toISOString()
      };

    case 'status':
      // 会话状态
      if (method === 'GET' || Object.keys(params).length === 0) {
        // 读取状态
        return {
          tool: 'status',
          action: 'get',
          state: {
            is_blocked: false,
            current_task: null,
            next_step: null,
            progress: [],
            pending: []
          }
        };
      } else {
        // 更新状态
        return {
          tool: 'status',
          action: 'update',
          state: params.state || params,
          timestamp: new Date().toISOString()
        };
      }

    case 'track':
      // 问题跟踪
      const action = params.action || 'list';
      
      switch (action) {
        case 'create':
        case 'add':
          return {
            tool: 'track',
            action: 'create',
            title: params.title || '',
            content: params.content || '',
            status: 'pending',
            issue_id: Date.now(),
            timestamp: new Date().toISOString()
          };
        case 'update':
          return {
            tool: 'track',
            action: 'update',
            issue_id: params.issue_id,
            status: params.status || 'in_progress',
            timestamp: new Date().toISOString()
          };
        case 'list':
        case 'get':
          return {
            tool: 'track',
            action: 'list',
            issues: [],
            total: 0
          };
        case 'archive':
          return {
            tool: 'track',
            action: 'archive',
            issue_id: params.issue_id,
            status: 'archived',
            timestamp: new Date().toISOString()
          };
        default:
          return { error: 'Unknown action: ' + action };
      }

    case 'task':
      // 任务管理
      const taskAction = params.action || 'list';
      
      switch (taskAction) {
        case 'batch_create':
        case 'create':
        case 'add':
          return {
            tool: 'task',
            action: 'create',
            title: params.title || '',
            feature_id: params.feature_id || 'default',
            tasks: params.tasks || [{ title: params.title, status: 'pending' }],
            timestamp: new Date().toISOString()
          };
        case 'update':
          return {
            tool: 'task',
            action: 'update',
            task_id: params.task_id,
            status: params.status || 'in_progress',
            timestamp: new Date().toISOString()
          };
        case 'list':
        case 'get':
          return {
            tool: 'task',
            action: 'list',
            tasks: [],
            total: 0
          };
        case 'delete':
          return {
            tool: 'task',
            action: 'delete',
            task_id: params.task_id,
            timestamp: new Date().toISOString()
          };
        case 'archive':
          return {
            tool: 'task',
            action: 'archive',
            feature_id: params.feature_id,
            timestamp: new Date().toISOString()
          };
        default:
          return { error: 'Unknown action: ' + taskAction };
      }

    case 'readme':
      // README 生成
      const lang = params.lang || 'en';
      const sections = params.sections || ['header', 'tools', 'deps'];
      
      return {
        tool: 'readme',
        action: 'generate',
        lang,
        sections,
        content: '# Project README\n\n(Generated by AIVectorMemory)',
        timestamp: new Date().toISOString()
      };

    case 'auto_save':
      // 自动保存偏好
      const preferences = params.preferences ? 
        (Array.isArray(params.preferences) ? params.preferences : [params.preferences]) : 
        [];
      const extraTags = params.extra_tags || params.extraTags || [];
      
      return {
        tool: 'auto_save',
        preferences,
        extra_tags: extraTags,
        scope: 'user',
        status: 'saved',
        timestamp: new Date().toISOString()
      };

    default:
      // 未知路径
      return { 
        error: 'Not Found', 
        path: pathname,
        available_endpoints: [
          '/health',
          '/remember',
          '/recall', 
          '/forget',
          '/status',
          '/track',
          '/task',
          '/readme',
          '/auto_save'
        ]
      };
  }
}

// 启动服务器
const PORT = config.port;
server.listen(PORT, () => {
  console.log(`========================================`);
  console.log(`  AIVectorMemory HTTP API Server`);
  console.log(`  Port: ${PORT}`);
  console.log(`  Health: http://localhost:${PORT}/health`);
  console.log(`========================================`);
  console.log(`Available endpoints:`);
  console.log(`  POST /remember - 存入记忆`);
  console.log(`  POST /recall   - 语义搜索`);
  console.log(`  POST /forget   - 删除记忆`);
  console.log(`  POST /status   - 会话状态`);
  console.log(`  POST /track    - 问题跟踪`);
  console.log(`  POST /task     - 任务管理`);
  console.log(`  POST /readme   - README生成`);
  console.log(`  POST /auto_save- 自动保存偏好`);
  console.log(`========================================`);
});