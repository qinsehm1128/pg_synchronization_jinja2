from cryptography.fernet import Fernet

# 生成新的加密密钥
key = Fernet.generate_key()
print("Generated encryption key:")
print(key.decode())
