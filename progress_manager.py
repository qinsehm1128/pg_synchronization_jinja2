"""进度管理器"""
import asyncio
import json
from typing import Dict, Any, Set
from fastapi import Request
from fastapi.responses import StreamingResponse
from typing import AsyncGenerator
import logging

logger = logging.getLogger(__name__)

class ProgressManager:
    """管理SSE客户端和进度更新"""
    
    def __init__(self):
        self.clients: Dict[int, Set[asyncio.Queue]] = {}  # job_id -> set of client queues
        self.current_progress: Dict[int, Dict[str, Any]] = {}  # job_id -> progress data
    
    def add_client(self, job_id: int, queue: asyncio.Queue):
        """添加SSE客户端"""
        if job_id not in self.clients:
            self.clients[job_id] = set()
        self.clients[job_id].add(queue)
        logger.info(f"Added SSE client for job {job_id}, total clients: {len(self.clients[job_id])}")
    
    def remove_client(self, job_id: int, queue: asyncio.Queue):
        """移除SSE客户端"""
        if job_id in self.clients:
            self.clients[job_id].discard(queue)
            if not self.clients[job_id]:
                del self.clients[job_id]
                # 清理进度数据
                self.current_progress.pop(job_id, None)
            logger.info(f"Removed SSE client for job {job_id}")
    
    def update_progress(self, job_id: int, progress: Dict[str, Any]):
        """更新进度并广播给所有客户端"""
        self.current_progress[job_id] = progress
        
        if job_id in self.clients:
            # 广播给所有客户端
            for queue in self.clients[job_id].copy():  # 使用copy避免迭代时修改
                try:
                    queue.put_nowait(progress)
                except asyncio.QueueFull:
                    logger.warning(f"Client queue full for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to send progress to client: {e}")
    
    def get_current_progress(self, job_id: int) -> Dict[str, Any]:
        """获取当前进度"""
        return self.current_progress.get(job_id, {})

# 全局进度管理器实例
progress_manager = ProgressManager()