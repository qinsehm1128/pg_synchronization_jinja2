"""
加密/解密工具模块
"""
from cryptography.fernet import Fernet
from config import settings
import base64
import logging

logger = logging.getLogger(__name__)

class EncryptionManager:
    """加密管理器"""
    
    def __init__(self):
        """初始化加密管理器"""
        try:
            # 确保密钥是有效的Fernet密钥格式
            if isinstance(settings.encryption_key, str):
                key_bytes = settings.encryption_key.encode()
            else:
                key_bytes = settings.encryption_key
            
            # 如果密钥不是标准的Fernet格式，尝试生成一个
            try:
                self.cipher = Fernet(key_bytes)
            except:
                # 如果密钥格式不正确，生成一个新的密钥
                logger.warning("Invalid encryption key format, generating new key")
                new_key = Fernet.generate_key()
                self.cipher = Fernet(new_key)
                logger.info(f"Generated new encryption key: {new_key.decode()}")
                
        except Exception as e:
            logger.error(f"Failed to initialize encryption: {e}")
            raise
    
    def encrypt(self, plaintext: str) -> str:
        """
        加密字符串
        
        Args:
            plaintext: 要加密的明文字符串
            
        Returns:
            加密后的字符串（Base64编码）
        """
        try:
            if not plaintext:
                return ""
            
            encrypted_bytes = self.cipher.encrypt(plaintext.encode('utf-8'))
            return base64.b64encode(encrypted_bytes).decode('utf-8')
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise
    
    def decrypt(self, encrypted_text: str) -> str:
        """
        解密字符串
        
        Args:
            encrypted_text: 要解密的加密字符串（Base64编码）
            
        Returns:
            解密后的明文字符串
        """
        try:
            if not encrypted_text:
                return ""
            
            encrypted_bytes = base64.b64decode(encrypted_text.encode('utf-8'))
            decrypted_bytes = self.cipher.decrypt(encrypted_bytes)
            return decrypted_bytes.decode('utf-8')
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

# 全局加密管理器实例
encryption_manager = EncryptionManager()

def encrypt_connection_string(connection_string: str) -> str:
    """加密数据库连接字符串"""
    return encryption_manager.encrypt(connection_string)

def decrypt_connection_string(encrypted_string: str) -> str:
    """解密数据库连接字符串"""
    return encryption_manager.decrypt(encrypted_string)

def generate_encryption_key() -> str:
    """生成新的加密密钥"""
    return Fernet.generate_key().decode()
