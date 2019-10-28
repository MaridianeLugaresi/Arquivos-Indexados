# Arquivos-Indexados

Disciplina de Algoritmos e Estrutura de Dados II

O objetivo é implementar organizações de Arquivos com índices reunindo uma grande quantidade de dados que serão buscados com o auxílio de uma API na rede social Twitter. Os dados serão armazenados como registros de tamanho fixo e após manipulados de forma binária com o auxílio da função seek.

Link para os arquivos de exemplo tweets usados na importação: https://gofile.io/?c=yIOK7M
Baixe e extraia na pasta dados do projeto.

Função main possui um menu de navegação com as seguintes opções:
	1 - Importar tweets dentro da pasta dados
	2 - Hashtags mais usadas
	3 - Buscar tweets com determinada hashtag
	4 - Buscar tweet por ID
	5 - Sair

A opção 1 chama a função Importa que grava os dados no diretório atual na pasta dados

A opção 2 chama a função topHashtags que internamente conta o número de ocorrências das hastags, ordena os Tweet's e depois mostra a posicao, o total de Tweets, o ID e o texto do Tweet.

A opção 3 chama a função tweetsPorHashtag que internamento faz uma busca por texto e uma busca por hashtag e depois mostra os dados do ID, o Nome, a Data, a Localidade e as Hashtags do proprio Tweet.

A opção 4 ainda não foi implementada.

A opção 5 sai do programa.

Para executar o programa em máquinas que não possuem a linguagem instalada, segue link para efetuar o download dos arquivos compilados:
https://github.com/MaridianeLugaresi/Arquivos-Indexados/releases/tag/v0.1.0