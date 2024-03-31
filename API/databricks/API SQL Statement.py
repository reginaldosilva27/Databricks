import requests
import json
import pandas as pd
from tabulate import tabulate
import matplotlib.pyplot as plt

url = f"https://adb-xxxx.13.azuredatabricks.net/api/2.0/sql/statements/"

headers = {
    'Authorization': "Bearer xxxx-3",
    "Content-Type": "application/json"
} 

data = {
    "warehouse_id": "xxxxxx",
    "statement": "select date_format(usage_end_time,'yyyy-MM') as Mes, \
                    sum(usage_quantity) as DBUs, \
                    (sum(usage_quantity) * max(c.pricing.default)) as TotalUSD \
                    from system.billing.usage a \
                    inner join system.billing.list_prices c on c.sku_name = a.sku_name \
                    group by all order by 1 desc limit 10",
    "wait_timeout": "5s"
} 

response = requests.post(
    url = url,
    headers=headers,
    data=json.dumps(data)
)

result = json.loads(response.content)

print("Status Code:", response.status_code)
print(json.dumps(result,indent=4))

# Extrair colunas e dados
columns = [col["name"] for col in result["manifest"]["schema"]["columns"]]
data = result["result"]["data_array"]

print(columns)
print(data)

# Criar DataFrame
df = pd.DataFrame(data, columns=columns)

# Print tabulado
print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))

df["TotalUSD"] = pd.to_numeric(df["TotalUSD"])

# Plotar o gráfico
plt.plot(df['Mes'], df['TotalUSD'],marker='o', linestyle='-', color='b')

# Adicionar rótulos e título
plt.xlabel('Mes')
plt.ylabel('TotalUSD')
plt.title('Consumo DBUS')

# Mostrar o gráfico
plt.grid(True)
plt.show()
