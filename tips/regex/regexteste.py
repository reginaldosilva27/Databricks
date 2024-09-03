import re
log_string = """
04:06:58  3 of 68 OK created sql incremental model bronze.vendors  [INSERT 0 2 in 6.70s]
"""
pattern1 = r"(\d{2}:\d{2}:\d{2})\s"                 # hora
pattern2 = r"(\d+)\s+of\s"                          # id
pattern3 = r"OK\s+created\s+sql\s+(\w+)\s+model\s"  # tipo
pattern4 = r"(\d+\.\d+)s\]"                         # tabela                                                    
pattern5 = r"(\d{2}:\d{2}:\d{2})\s+(\d+)\s+of\s+\d+\s+OK\s+created\s+sql\s+(\w+)\s+model\s+([\w\.]+)\s+.*?\[.*?in\s+(\d+\.\d+)s\]" # todas colunas

print("--------------------------------------")
print(re.search(pattern1, log_string))
print(re.search(pattern1, log_string).group(1))
print("--------------------------------------")
print(re.search(pattern2, log_string))
print(re.search(pattern2, log_string).group(1))
print("--------------------------------------")
print(re.search(pattern3, log_string))
print(re.search(pattern3, log_string).group(1))
print("--------------------------------------")
print(re.search(pattern4, log_string))
print(re.search(pattern4, log_string).group(1))
print("--------------------------------------")
print(re.search(pattern5, log_string))
print(re.search(pattern5, log_string).group(1))
print(re.search(pattern5, log_string).group(2))
print(re.search(pattern5, log_string).group(3))
print(re.search(pattern5, log_string).group(4))
print(re.search(pattern5, log_string).group(5))