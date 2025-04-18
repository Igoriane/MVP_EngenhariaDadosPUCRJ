# MVP_EngenhariaDadosPUCRJ

## Link para o Databricks publicado:
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1306962464841166/540095536058281/8644182617735510/latest.html
O código está disponível no arquivo MVP.ipynb também

---

## 1. Ingestão e Organização Inicial

- **Ingestão dos CSV:**  
  Os arquivos CSV do Northwind foram carregados manualmente para o Databricks e, em seguida, lidos via PySpark com as opções `header` habilitado e `inferSchema` ativado.  
  Cada arquivo foi salvo como uma tabela Delta na Camada Bronze, com a coluna adicional `data_ingestao` (inicialmente em formato timestamp).

- **Objetivo Inicial:**  
  Preservar o dado original, adicionando um controle temporal da ingestão.

---

## 2. Transformações Realizadas na Camada Silver

### 2.1. Renomeação de Colunas

- **Objetivo:**  
  Padronizar todos os nomes de colunas para o formato _snake_case_ e em minúsculo, facilitando a consulta e o entendimento dos relacionamentos.
  
- **Exemplo:**  
  Converti `CustomerID` para `customer_id`, `OrderID` para `order_id`, `EmployeeID` para `employee_id`, dentre outros.

---

### 2.2. Conversão de Tipos de Dados

- **Datas:**  
  Todas as colunas de data, como `order_date`, `required_date`, `shipped_date`, `birth_date` e `hire_date`, foram convertidas do formato timestamp para o tipo `date`, removendo a parte de hora.  
  *Exemplo:* Na tabela `order`, o campo `order_date` passou a ter apenas a data (e.g., *1996-07-04*).

- **Números:**  
  Converto colunas numéricas para os tipos apropriados.  
  *Exemplo:* Em `order_details`, o campo `unit_price` foi convertido para `double`, `quantity` para `int` e `discount` para `double`.

---

### 2.3. Remoção de Colunas Irrelevantes

- **Employees:**  
  Excluí as colunas `photo_path`, `notes` e `title_of_courtesy`, que não eram necessárias para a análise, de modo a reduzir ruídos e simplificar o modelo.

---

### 2.4. Tratamento de Delimitadores e Problemas de Leitura

- **Problema Identificado:**  
  Nas tabelas que continham endereços como `orders` e `suppliers`, campos de texto, como `ship_address` e `ship_city`, continham vírgulas não encapsuladas que interferiam na correta interpretação das colunas durante a leitura dos CSV.
  
- **Solução Aplicada:**  
  Configurei a leitura dos arquivos CSV com as opções adequadas para tratamento de caracteres de citação. Usei o parâmetro `quote` (definido como `"`) para assegurar que os valores delimitados por aspas fossem interpretados como um único campo, mesmo que contenham vírgulas, além de parsings para a customização dos campos de modo que a padronização fosse respeitada.

---

### 2.5. Conversão e Padronização da Coluna de Ingestão

- **data_ingestao:**  
  Originalmente inserida como timestamp na ingestão dos dados, converti essa coluna para o tipo `date` em todas as tabelas.  
  Esse ajuste garante um formato consistente para análises temporais e padroniza a informação de controle da carga. Entendendo que para essa análise não precisaríamos do timestamp, no entanto, reconheço que num cenário real essa informação poderia ser necessária para o negócio.

---

### 2.6. Reorganização das Colunas

- **Objetivo:**  
  Após as transformações, reorganizei a ordem das colunas (quando necessário) para manter uma sequência lógica (por exemplo, chaves primárias e datas de forma consistente), facilitando a leitura e a compreensão do dado final.

---

## 3. Verificação e Validação (Qualidade do Dado)

- **Valores Nulos e Duplicados:**  
  Verifiquei que as colunas consideradas essenciais (como `customer_id` em `customers` e `order_id` em `orders`) não possuem valores nulos. Registros duplicados foram filtrados conforme a necessidade.
  No entanto em algumas tabelas como a de região, haviam muitos nulos que não faziam sentido serem desprezados.

- **Consistência de Domínios:**  
  Gere estatísticas descritivas para os campos numéricos (ex.: `unit_price`, `freight`) e validei os intervalos esperados.  
  Para os campos categóricos (ex.: `country`, `city`), listei os valores distintos para confirmar a padronização.

- **Integridade Referencial:**  
  Efetuei joins de verificação (como o left anti join entre `orders` e `customers`) para assegurar que os relacionamentos entre as tabelas estejam corretos. Nenhum registro órfão foi encontrado.


---

## 4. Conclusão da Camada Silver

Após a transformação na camada Silver, o dado se encontra:
- **Padronizado:** Todos os nomes de coluna estão em _snake_case_ e em minúsculo.
- **Tipado Corretamente:** Datas em formato `date` e campos numéricos convertidos para os tipos adequados.
- **Limpo:** Colunas irrelevantes foram removidas, e os problemas de leitura de CSV (como vírgulas em textos) foram solucionados.
- **Integrado:** Os relacionamentos entre as tabelas estão íntegros, o que possibilita a construção de um modelo de dados confiável (esquema estrela) para o Data Warehouse.
- **Valores Nulos:** Foi possível notar algumas colunas com valores nulos que podem descrever uma possível ausência de informação, isso pode ser classificado como um problema na base de dados que o negócio precisa ter conhecimento para tratamento no sistema principal.

## 5. Carga dos Dados para Camada Gold

Além das transformações detalhadas na camada Silver, realizei a carga dos dados para o Data Warehouse final (camada Gold) utilizando o comando `saveAsTable` do Databricks. Essa etapa garante que os dados limpos e validados sejam registrados de forma permanente e possam ser consultados por meio de SQL e assim as respostas das perguntas iniciais sejam realizadas. 

```python
# Exemplo: Carga dos dados para o Data Warehouse
spark.sql("CREATE DATABASE IF NOT EXISTS northwind_dw")
tabelas = ["categories", "customers", "employees", "employee_territories", "order_details", "orders", "products", "regions", "shippers", "suppliers", "territories"]
caminho_prata = "/FileStore/silver/"

for tabela in tabelas:
    df = spark.read.format("delta").load(f"{caminho_prata}{tabela}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"northwind_dw.{tabela}")
```

# 6. Solução do Problema

Nesta etapa, apresento as respostas às perguntas de negócio definidas no início. A seguir, descrevo cada pergunta, a consulta técnica utilizada para obtê-la e a discussão dos resultados.

## Pergunta 1: Como a empresa está desempenhando ao longo do tempo? Existe sazonalidade nas vendas? Existe evolução ou involução ao longo dos anos?

## Solução do Problema

### Tendência de Vendas ao Longo do Tempo

A consulta abaixo foi utilizada para agregar o total das vendas por mês, permitindo identificar tendências sazonais:

```sql
%sql
SELECT 
    date_trunc('month', order_date) AS mes,
    SUM(od.quantity * od.unit_price) AS total_vendas
FROM northwind_dw.orders o
JOIN northwind_dw.order_details od 
  ON o.order_id = od.order_id
GROUP BY date_trunc('month', order_date)
ORDER BY mes;
```

Foi possível notar uma leve sazonalidade negativa nos meses de meio de ano. Enquanto que o mês de abril de 1998 foi o de melhor desempenho, esse desempenho foi acompanhado pelos seus meses precursores desde o mês de Outubro de 1997. Também ficou claro que o desempenho da empresa vem crescendo no longo prazo, uma vez que o ano de 1996 é o ano que está com piores desempenhos, enquanto que o ano de 1998 é o com melhor desempenho.

## Pergunta 2: Qual a região mais lucrativa para a empresa? Ela deve investir em lojas em qual região?

## Solução do Problema

### Vendas por Região

A consulta abaixo foi utilizada para tentar identificar qual a região que rende mais vendas a empresa:

```sql
%sql
SELECT 
    c.region AS regiao,
    SUM(od.quantity * od.unit_price) AS total_vendas
FROM northwind_dw.orders o
JOIN northwind_dw.order_details od 
  ON o.order_id = od.order_id
JOIN northwind_dw.customers c 
  ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY total_vendas DESC;
```

Essa foi uma das análises que foi prejudicada pela má ingestão de dados, provavelmente proveniente de um campo livre no sistema interno da empresa. Isso por que a coluna de região tem diversas informações nulas e/ou pouco padrozinadas. Enquanto Rio de Janeiro e Québec são regiões, temos estados com siglas como WA e OR. Dessa forma pouco pode ser presumido com relação a qual região que gera mais lucros. O fato é que podemos apenas afirmar que a região que tem a melhor padronização versus vendas parece ser a do Rio de Janeiro. No entanto o analista de dados deve refletir se Rio de Janeiro e São Paulo não deveriam ser tratados como uma região única chamada Brazil, ou até mesmo América do Sul ou Latina. Isso por que a empresa não é Brasileira e isso pode refletir melhor a sua posição global.

## Pergunta 3: Qual o tipo de produto ao qual é mais vendido pela empresa? Existe um desempenho semelhante? Pode-se dizer que a região com mais vendas impacta no produto mais vendido?

## Solução do Problema

### Contribuição de Cada Categoria nas Vendas

A consulta abaixo foi utilizada para chegar a conclusão de qual das categorias de produtos vendidos pela empresa gera mais lucro:

```sql
%sql
SELECT 
    cat.category_name,
    SUM(od.quantity * od.unit_price) AS total_vendas_categoria
FROM northwind_dw.order_details od
JOIN northwind_dw.products p 
  ON od.product_id = p.product_id
JOIN northwind_dw.categories cat 
  ON p.category_id = cat.category_id
GROUP BY cat.category_name
ORDER BY total_vendas_categoria DESC;
```

Por fim, essa é uma análise que foi possível ser feita de modo completo uma vez que a tabela de categorias é uma das tabelas mais consistentes da base de dados da Northwind. É possível organizar o altíssimo desempenho de bebidas e produtos do dia a dia frente aos outros produtos da empresa. Uma vez que a categoria Bebidas demonstrou um ganho de cerca de 60% frente as carnes que são o 3º produto mais vendido pela empresa. Enquanto os dois produtos menos vendidos combinados não consegue nem chegar ao nível do segundo produto mais vendido, Dairy Products (produtos do dia a dia)


# 7. Conclusão

# Conclusão

# Conclusão

Ao longo deste projeto, a transformação aplicada na camada Silver evidenciou não apenas a viabilidade de um pipeline ETL robusto, mas também os desafios práticos que se impõem ao tratar dados provenientes de fontes heterogêneas. A padronização das colunas, a conversão dos tipos de dados e o cuidado com os delimitadores demonstraram a complexidade intrínseca à organização dos dados brutos em um formato consistente e confiável.

Além disso, a escolha de utilizar DW para armazenar os dados transformados evidenciou uma abordagem proativa em termos de governança e versionamento dos dados. Esse formato não apenas facilitou a reprocessamento e auditoria do pipeline, como também garantiu operações transacionais seguras, o que é fundamental para ambientes que demandam escalabilidade e confiabilidade.

A reestruturação do pipeline em camadas – diferenciando a ingestão bruta, a transformação aprofundada e, finalmente, a carga para o Data Warehouse – permitiu um controle detalhado do fluxo de dados, onde cada etapa é validada tanto em termos de integridade quanto de consistência. Essa divisão modular contribuiu para a identificação precoce de inconsistências e oferece uma base sólida para a implementação de melhorias contínuas uma vez que foi possível realizar transformações tanto na camada bronze quanto na prata.

Resumindo, a experiência adquirida durante a transformação e a validação dos dados reforça a importância de uma estratégia de ETL com cuidado, que ultrapassa a mera execução de comandos, integrando um pensamento crítico sobre a qualidade dos dados e os desafios de harmonizá-los. E que mesmo com todo o cuidado pode-se ainda não ter uma análise consistente de todos os fatores caso a base de dados não seja íntegra o suficiente. Não é suficiente que as áreas de negócio e que a gerência apenas queiram fazer uma análise de dados, ou desejem ter as ultimas tecnologias. É necessário que exista um comprometimento completo com a integridade dos dados.



