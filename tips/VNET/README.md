### VNET (Virtual Network):
A VNET é uma rede privada no Azure que permite o isolamento de recursos e a comunicação segura entre eles. No contexto de Databricks, a VNET Injection permite que o cluster seja implantado dentro de uma VNET do cliente, oferecendo maior controle sobre o tráfego de rede e a conectividade com recursos externos.

### Subnets:
As sub-redes (subnets) dividem uma VNET em segmentos menores. Elas são utilizadas para isolar recursos, controlar o tráfego e aplicar regras de segurança específicas. No Databricks, diferentes subnets podem ser usadas para isolar a comunicação entre clusters e outros serviços.

### Network Security Groups (NSG):
Um NSG contém regras de segurança que controlam o tráfego de rede de entrada e saída para os recursos de uma subnet ou interface de rede. Essas regras ajudam a proteger os recursos dentro da VNET, permitindo ou bloqueando o tráfego com base em endereços IP, portas e protocolos.

### Private Endpoint:
Um Private Endpoint cria uma interface de rede privada dentro de uma subnet para conectar-se a serviços do Azure (como Databricks ou o Azure Storage) sem expor o tráfego à internet pública. Isso melhora a segurança ao garantir que toda a comunicação aconteça dentro da rede privada.

### Private DNS Servers:
Para que os recursos dentro de uma VNET resolvam corretamente nomes de domínio associados a Private Endpoints, é necessário configurar servidores DNS privados. Esses servidores permitem a resolução de endereços IP internos e externos, garantindo a comunicação adequada dentro da infraestrutura isolada.

### Peering:
O VNET Peering conecta duas redes virtuais no Azure, permitindo que elas se comuniquem diretamente sem a necessidade de gateways ou roteamento através da internet. No cenário de Databricks, o peering pode ser utilizado para conectar a VNET onde o workspace está injetado com outras VNETs que hospedam recursos críticos.

### VPN Gateway:
Um gateway VPN oferece conectividade segura entre a rede on-premises e a VNET do Azure através de uma conexão encriptada (IPsec). Isso permite que os recursos no Azure se conectem à infraestrutura local de forma segura e privada, útil para cenários híbridos.

### ExpressRoute:
O ExpressRoute é uma solução de conectividade privada que permite conexões dedicadas e de baixa latência entre a rede on-premises e o Azure, sem passar pela internet pública. É geralmente usada para cargas de trabalho sensíveis e de alta performance, garantindo maior confiabilidade e segurança.

### Route Tables:
As tabelas de rota controlam como o tráfego é direcionado dentro de uma VNET. Elas definem rotas personalizadas, permitindo que o tráfego seja direcionado para diferentes subnets, gateways ou outros destinos. No Databricks, as route tables podem ser usadas para garantir que o tráfego siga por caminhos seguros e otimizados.

### NAT Gateway:
O NAT Gateway permite que recursos dentro de uma subnet privada acessem a internet de forma segura, mascarando seus endereços IP com um único endereço público de saída. Isso é útil para controlar o tráfego de saída e limitar a exposição direta dos recursos internos à internet pública.
