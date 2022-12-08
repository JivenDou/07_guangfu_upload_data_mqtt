"""
@File  : AES_new.py
@Author: lee
@Date  : 2022/3/8/0008 9:41:14
@Desc  :
"""
import base64

# import wmi
from Crypto.Cipher import AES

passwd = "123456781234567"
iv = '1234567812345678'


def add_16(par):
    if type(par) == str:
        par = par.encode()
    while len(par) % 16 != 0:
        par += b'\x00'
    return par


class AESCrypt:
    def __init__(self, key, model, iv=''):
        self.key = add_16(key)
        self.model = model
        self.iv = add_16(iv)

    def aesencrypt(self, text):
        text = add_16(text)
        aes = None
        if self.model == AES.MODE_CBC:
            aes = AES.new(self.key, self.model, self.iv)
        elif self.model == AES.MODE_ECB:
            aes = AES.new(self.key, self.model)
        encrypt_text = aes.encrypt(text)
        return base64.encodebytes(encrypt_text).decode('utf8').rstrip("\n")

    def aesdecrypt(self, text):

        if self.model == AES.MODE_CBC:
            aes = AES.new(self.key, self.model, self.iv)
        elif self.model == AES.MODE_ECB:
            aes = AES.new(self.key, self.model)
        try:
            decrypt_text = text.encode('utf8')
            decrypt_text = base64.decodebytes(decrypt_text)
            decrypt_text = aes.decrypt(decrypt_text)
            decrypt_text = decrypt_text.strip(b"\x00")
            return decrypt_text.decode('utf8')
        except Exception as e:
            print(e)
            return None


# def get_cpu_code():
#     try:
#         c = wmi.WMI()
#         for cpu in c.Win32_Processor():
#             cpu_code = cpu.ProcessorId.strip()
#         return cpu_code
#     except Exception as e:
#         return None


def decrypt(text):
    aes_cryptor = AESCrypt(passwd, AES.MODE_CBC, iv)  # CBC模式
    return aes_cryptor.aesdecrypt(text)


def encrypt(text):
    aes_cryptor = AESCrypt(passwd, AES.MODE_CBC, iv)
    return aes_cryptor.aesencrypt(text)


if __name__ == '__main__':
    passwd = "123456781234567"
    iv = '1234567812345678'

    # aescryptor = AESCrypt(passwd, AES.MODE_CBC, iv)  # CBC模式
    aescryptor = AESCrypt(passwd,AES.MODE_ECB,"") # ECB模式
    text = "zzZZ4144670.."
    en_text = aescryptor.aesencrypt(text)
    print("密文:", en_text)
    text = aescryptor.aesdecrypt(en_text)
    print("明文:", text)
