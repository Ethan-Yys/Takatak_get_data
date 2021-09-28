#coding=utf-8
# 从chenhe召回中看到的hashcode
def javaHashCode(s):
    s = s.decode("utf-8")
    INT_31 = 2147483648  # 1 << 31
    INT_32 = 4294967296  # INT_31 << 1
    hash_value = 0
    for c in s:
        hash_value = 31 * hash_value + ord(c)
    int_out = int(((hash_value + INT_31) % INT_32 - INT_31) % 10000)  # 溢出处理
    return int_out + 1

def convert_n_bytes(n, b):
    bits = b * 8
    return (n + 2**(bits-1)) % 2**bits - 2**(bits-1)

def convert_4_bytes(n):
    return convert_n_bytes(n, 4)

def hash_code(s):
    h = 0
    n = len(s)
    for i, c in enumerate(s):
        h = h + ord(c) * 31 ** (n-1-i)
    return convert_n_bytes(h, 4)

def get_flow_num(s, suff="", module=10000):
    return abs(hash_code(s+suff) + 1) % module

if __name__ == "__main__":
    s_list = [
        "{fb}{1008262242900801}",
        "{fb}{1016746475351735}",
        "{fb}{1018845615120658}",
        "{fb}{10214908955984067}",
        "{fb}{10221263194431188}",
        "{fb}{1035006290182733}",
        "9e58b175-4760-4327-b636-aa59cb57fa2a941115",
        "90b7393a-64d2-4db4-b042-d3af395b8c9c8401899",
        "b96c4b05-d4e3-4457-af94-8b80d49dff7d7091898",
        "6ce5df6c-d1bf-49e4-af9a-9f2d221dfede9497209",
        "19663586-e888-491f-8216-c591521b89b6367257294",
        "b2ad1766-a686-467c-8df5-26710540eb698313846",
    ]
    for s in s_list:
        print(s, get_flow_num(s+"_fetch_tabs_version_4_0"))
        print(s, javaHashCode( (s+"_fetch_tabs_version_4_0").encode('utf-8') ))
        print('\n')
