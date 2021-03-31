# elasticsearch-integration-jupyter

Repositório com a integração entre Jupyter Notebook e Elasticsearch.

**Pastas**
<b></b>
- Na pasta Data, há a base de dados books.csv. Essa base de dados foi extraída do Kaggle e usada aqui para ilustrar o envio de dados ao Elasticsearch via Jupyter Notebook
- Na pasta notebooks, há o arquivo .ipynb, ou seja, o notebook em si.

**Uso**
<b></b>
1. Baixe o Elasticsearch.
2. Baixe o Kibana.
3. Extraia os arquivos.
4. Crie um arquivo .bat para inicializar o elasticsearch.bat e o kibana.bat juntos. Da forma:
<b></b>
```
echo Starting ELK Stack
START cmd /C <caminho-do-arquivo>\bin\elasticsearch.bat
echo Starting Kibana
START cmd /C <caminho-do-arquivo>\bin\kibana.bat
```
5. Execute o arquivo .bat criado.
6. Após um breve tempo, verifique se o Elasticsearch e o Kibana estão rodando. Elasticsearch roda em localhost:9200 e o Kibana roda em localhost:5601.
7. Execute o Jupyter Notebook para enviar os dados do arquivo csv ao Elasticsearch pela integração entre Jupyter Notebook e o Elasticsearch.
