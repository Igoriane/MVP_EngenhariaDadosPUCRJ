# MVP_EngenhariaDadosPUCRJ

# Análise de Qualidade e Transformação do Dado – Camada Silver

Nesta seção, descrevo o procedimento que realizei para transformar o dado bruto (Camada Bronze) em dado limpo, padronizado e apto para ser carregado no Data Warehouse. O foco é demonstrar a qualidade do dado e justificar os ajustes aplicados durante a etapa de transformação (Camada Silver).

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

## 4. Conclusão

Após a transformação na camada Silver, o dado se apresenta:
- **Padronizado:** Todos os nomes de coluna estão em _snake_case_ e em minúsculo.
- **Tipado Corretamente:** Datas em formato `date` e campos numéricos convertidos para os tipos adequados.
- **Limpo:** Colunas irrelevantes foram removidas, e os problemas de leitura de CSV (como vírgulas em textos) foram solucionados.
- **Integrado:** Os relacionamentos entre as tabelas estão íntegros, o que possibilita a construção de um modelo de dados confiável (esquema estrela) para o Data Warehouse.
- **Valores Nulos:** Foi possível notar algumas colunas com valores nulos que podem descrever uma possível ausência de informação, isso pode ser classificado como um problema na base de dados que o negócio precisa ter conhecimento para tratamento no sistema principal.

# Solução do Problema

Nesta etapa, apresento as respostas às perguntas de negócio definidas no início do projeto. A seguir, descrevo cada pergunta, a consulta técnica utilizada para obtê-la e a discussão dos resultados.

## Pergunta 1: Qual é a tendência das vendas ao longo do tempo?

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
