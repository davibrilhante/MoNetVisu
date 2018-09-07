# MoNet Visu
Os códigos hospedados neste repositório são de autoria de Davi da Silva Brilhante, em comprimento dos requisitos da disciplina 
TEBDI do Curso de Pós graduação em Eng. de Sistemas e Computaão da COPPE UFRJ.

O MoNet Visu (Mobile Network Big Data Visualization) é uma aplicação de big data para vizualização de dados em redes móveis a 
partir de data sets de mobilidade e posicionamento de ERBS (Estação Rádio Base).

# Pré-requisitos
  Spark
  Matplotlib
  Shapely
  Operator
  Ipython
  Jupyter

# Como Utilizar
1 . Descompacte os arquivos no data set de ERBS

2 . Execute o script "shezen-erbs.py" com "spark-submit". O script espera como argumento o tipo de visualização desejado, que podem ser valores de 0 a 4.

Para executar com o Jupyter Notebook, basta executá-lo na pasta onde está o arquivo "shenzen-ipy.ipynb"

# Sobre os Datasets
Os datasets foram obtidos em plataformas opensource, sem custos desde que não sejam usados para fins lucrativos e seja reconecida
a autoria.

Os datasets das ERBS foi obtido em: https://www.opencellid.org/stats.php

Os datasets de mobilidade foram obtidos em: https://www-users.cs.umn.edu/~tianhe/BIGDATA/

Os autores deste trabalho reconhecem a autoria destes datasets de acordo com o exposto nas páginas listadas acima.
