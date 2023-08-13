""" 
A criação do desafio foi analisada e criado baseado no vídeo do Expert/professor.
Tal ação foi realizada devído eu não conseguir cadastrar a forma de pagamento na conta GCP do google cloud, deu erro.
Dessa forma estou realizando o fork e gerando as alterações de códificação da realização do desafio.

O arquivo resultado.txt não será realizado dévido a falta de acesso ao GCP, mais o código abaixo faz isso.
Entrega do Resultado
Criar um repositório no GitHub.
Criar um arquivo chamado resultado.txt. 
Dentro desse arquivo, colocar as 10 palavras que mais são usadas no livro, 
de acordo com o resultado do Job.
Inserir os arquivo resultado.txt e part-00000 no repositório e informar na plataforma da Digital Innovation One.
"""

Criação do projeto de forma gráfica na plataforma google cloud, conforme aula: desafio-dio
Criação do Cluster de forma gráfica na plataforma google cloud, conforme aula: desafio-wordcount 
Criação do Bucket de forma gráfica na plataforma google cloud, conforme aula: desafio_bucket 
Criação do Bucket de forma gráfica na plataforma google cloud, conforme aula: desafio

# Implementando Desáfio

''' 
1 - Clonar repositório do Github
Então com o GCP aberto, abrimos o console do Cloud shell
Copiamos o link do repositorio do git que desejamos clonar
Com o cluster aberto damos um git clone no cloud shell corforme comando abaixo
'''
desafio-wordcount&cloudshell:~ (desafio-wordcount)$ git clone https://github.com/marcelomarques05/dio-desafio-dataproc
# Feito o clone do dir acesso o mesmo conforme comando abaixo
desafio-wordcount&cloudshell:~ (desafio-wordcount)$ cd dio-desafio-dataproc
# Visualizando arquivos do dir
desafio-wordcount&cloudshell:~/dio-desafio-dataproc (dio-desafio-dataproc)$ ls -la

# 2 - Agora vamos acessar o Bucket, para visualizar o nome do mesmo damos os seguinte comandos, o nome do backet será apresentado
desafio-wordcount&cloudshell:~/dio-desafio-dataproc (dio-desafio-dataproc)$ gsutil ls

# 3 - Atualizar o arquivo contador.py com o nome do Bucket criado nas linhas que contém {SEU_BUCKET}.
desafio-wordcount&cloudshell:~/dio-desafio-dataproc (dio-desafio-dataproc)$ vim contador.py

# Uma vez que o arquivo é exibido atualizo o nome do bucket com a pasta resultado conforme informações abaixo
# Feito as alterações salvo
import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - desafio-wordcount ")
    words = sc.textFile("gs://desafio_bucket/livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    wordCounts.saveAsTextFile("gs://desafio_bucket/resultado")

# 4 - Fazer o upload dos arquivos contador.py e livro.txt para o bucket criado (instruções abaixo)
# Agora o bucket tem esses dois arquivos(contador.py livro.txt)
desafio-wordcount&cloudshell:~/dio-desafio-dataproc (dio-desafio-dataproc)$ gsutil cp contador.py livro.txt gs://desafio_bucket/

# 5 - Utilizar o código em um cluster Dataproc, executando um Job do tipo PySpark chamando gs://{SEU_BUCKET}/contador.py
'''
Agora minimizamos o console do cloud shell, 
Vamos até o cluster na plataforma do GCP.
Do lado esquerdo clicamos em jobs, damos um nome pra o job chamado: desafio.
Depois escolho o cluster: desafio-wordcount
Depois o tipo de jobs: Pyspark
Depois na opção:
Arquivo python principal: gs://desafio_bucket contador.py

Agora clicamos em enviar para rodar o Job'''

# Após a executa acima com sucesso ele  gera 2 arquivos na pasta resultado no bucket.

O Job irá gerar uma pasta no bucket chamada resultado. 
Dentro dessa pasta o arquivo part-00000 irá conter a lista de palavras e 
quantas vezes ela é repetida em todo o livro.


Desde de já obrigado!!!

Att,
Genivon Silva



    





