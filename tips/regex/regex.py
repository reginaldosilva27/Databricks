import re
import csv
from tabulate import tabulate

log_string = """
04:06:51  1 of 25 START sql incremental model bronze.users  [RUN]
04:06:51  2 of 25 START sql incremental model bronze.prices  [RUN]
04:06:51  3 of 25 START sql incremental model bronze.vendors  [RUN]
04:06:51  4 of 25 START sql table model bronze.customers  [RUN]
04:06:58  3 of 25 OK created sql incremental model bronze.vendors  [INSERT 0 2 in 6.70s]
04:06:58  5 of 25 START sql incremental model bronze.orders  [RUN]
04:06:58  4 of 25 OK created sql table model bronze.customers  [SELECT in 6.94s]
04:06:58  6 of 25 START sql incremental model bronze.teste  [RUN]
04:07:00  2 of 25 OK created sql incremental model bronze.prices  [INSERT 0 133 in 8.31s]
04:07:00  7 of 25 START sql table model bronze.email .............. [RUN]
04:07:06  1 of 25 OK created sql incremental model bronze.users  [INSERT 0 178089 in 14.30s]
04:07:06  8 of 25 START sql view model bronze.sales  [RUN]
04:07:10  5 of 25 OK created sql incremental model bronze.orders  [INSERT 0 5 in 1200.90s]
04:07:10  9 of 25 START sql view model bronze.people  [RUN]
04:07:13  8 of 25 OK created sql view model bronze.sales  [CREATE VIEW in 74.74s]
04:07:13  10 of 25 START sql view model bronze.transfers ... [RUN]
04:07:18  9 of 25 OK created sql view model bronze.people  [CREATE VIEW in 8.04s]
04:07:18  11 of 25 START sql view model bronze.employees  [RUN]
04:07:21  10 of 25 OK created sql view model bronze.transfers  [CREATE VIEW in 700.72s]
04:07:21  12 of 25 START sql incremental model bronze.undefined .. [RUN]
04:07:23  11 of 25 OK created sql view model bronze.employees  [CREATE VIEW in 80.90s]
"""

# Criando lista, quebrando por quebra de linha
logs = log_string.split("\n")

# Filtrando apenas eventos de finalização
logs = list(filter(lambda x: "OK created" in x, logs))

# Regra Regex para extrair informações necessárias
pattern = r"(\d{2}:\d{2}:\d{2})\s+(\d+)\s+of\s+\d+\s+OK\s+created\s+sql\s+(\w+)\s+model\s+([\w\.]+)\s+.*?\[.*?in\s+(\d+\.\d+)s\]"

# Criando um loop para processar cada log
log_data = []
for log in logs:
    match = re.search(pattern, log)
    if match:
        start_time = match.group(1)
        task_number = int(match.group(2))
        model_type = match.group(3)
        model_name = match.group(4)
        duration_seconds = float(match.group(5))
        duration_minutes =  round(duration_seconds / 60,2)
        
        # Adicionando os dados à lista
        log_data.append([start_time, task_number, model_type, model_name, duration_seconds, duration_minutes])
    else:
        print("Log não corresponde ao padrão esperado.")

# Ordenando pelo mais demorado
log_data.sort(key=lambda x: x[4], reverse=True)

# Printando na tela em formato tabular
print(tabulate(log_data, headers=["start", "task", "type", "model", "duration_sec","duration_min"]))

# Gerando CSV
csv_file = "/Users/reginaldosilva/Downloads/log_data.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["start", "task", "type", "model", "duration_sec","duration_min"])
    writer.writerows(log_data)