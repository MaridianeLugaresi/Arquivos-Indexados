package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os" //para trabalhar com arquivos
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/maridianelugaresi/arquivos-indexados/banco"
)

type db struct {
	tweets   *banco.TabelaTweets
	hashtags *banco.TabelaHashtags
}

func (b *db) importa(diretorio string) error {
	// https://progolang.com/listing-files-and-directories-in-go/
	//retorna em arquivos todos aquele que encontrou
	arquivos, err := filepath.Glob(filepath.Join(diretorio, "*.data"))
	if err != nil {
		return err
	}
	for _, arquivo := range arquivos { //faz o for passando cada arquivo de arquivos
		//Abrindo o arquivo
		log.Printf("Iniciando importação do arquivo: %s", arquivo)
		f, err := os.Open(arquivo)
		if err != nil { //nil é nulo
			return err
		}
		// funções chamadas no defer são executadas ao sair do escopo atual
		defer f.Close()

		buffer := bufio.NewScanner(f)
		for buffer.Scan() { //é um booleano
			line := buffer.Text() //retorna a linha que leu
			t := twitter.Tweet{}
			erro := json.Unmarshal([]byte(line), &t) //decodifica o json e retorna um erro
			if erro != nil {
				continue
			}

			data, erro := t.CreatedAtTime() //reetorna a data separada
			if erro != nil {
				continue
			}

			hashtags := []banco.Hashtag{}
			for _, h := range t.Entities.Hashtags {
				hashtag := banco.Hashtag{}
				texto := strings.ToLower(h.Text)
				err := b.hashtags.BuscaPorTexto(texto, &hashtag)
				if err != nil && err != banco.NaoEncontrado {
					log.Fatalf("Erro desconhecido ao buscar hashtag, err: %v", err)
				}

				if err == banco.NaoEncontrado {
					// Se não encontrar a hashtag cadastramos ela
					hashtag.Texto = texto
					err := b.hashtags.Inserir(&hashtag)
					if err != nil {
						log.Fatalf("Erro desconhecido ao salvar hashtag, err: %v", err)
					}
				}
				hashtags = append(hashtags, hashtag)
			}

			registro := banco.Tweet{
				ID:         t.ID,
				Nome:       t.User.ScreenName,
				Localidade: t.User.Location,
				Data:       data,
				Hashtags:   hashtags,
				Mensagem:   t.Text,
			}

			err = b.tweets.Inserir(registro)
			if err != nil {
				log.Printf("Erro ao salvar Tweet: %v", err)
			}
		}
	}
	return nil
}

func (b *db) topHashtags() error {
	hashtagsMap, err := b.hashtags.ListaHashtagsComCounts()
	if err != nil {
		return fmt.Errorf("Erro na busca de hashtags, err: %v", err)
	}
	maximoRegistros := len(hashtagsMap)
	if len(hashtagsMap) > 500 {
		fmt.Printf("Total de hashtags: %d, mas mostrando apenas 500\n", maximoRegistros)
		maximoRegistros = 500
	}
	fmt.Printf("Ordenando registros\n")
	hashtags := make([]*banco.Hashtag, 0, len(hashtagsMap))
	for _, hashtag := range hashtagsMap {
		hashtags = append(hashtags, hashtag)
	}
	sort.Slice(hashtags, func(i, j int) bool { return hashtags[i].TotalTweets > hashtags[j].TotalTweets })
	hashtags = hashtags[0:maximoRegistros]
	fmt.Printf("Termino ordenacao dos registros\n")
	fmt.Printf("Posicao | TotalTweets | ID | Texto\n")
	for i, h := range hashtags {
		fmt.Printf("%d | %d | %d | %s\n", i+1, h.TotalTweets, h.ID, h.Texto)
	}
	return nil
}

func (b *db) tweetsPorHashtag(texto string) {
	hashtag := banco.Hashtag{}
	err := b.hashtags.BuscaPorTexto(texto, &hashtag)
	if err != nil {
		log.Printf("Erro buscando hashtag, err: %v", err)
	}
	tweets, err := b.tweets.BuscaPorHashtag(hashtag.ID)
	if err != nil {
		log.Printf("Erro buscando tweets, err: %v", err)
	}

	for _, t := range tweets {
		hashtags := ""
		for _, hashtag := range t.Hashtags {
			hashtags += "[#" + hashtag.Texto + "]"
		}
		fmt.Printf("\nID: %d Nome: %s Data: %s Localidade: %s\n", t.ID, t.Nome, t.Data.Format(time.RFC3339), t.Localidade)
		fmt.Printf("Hashtags: %s\n", hashtags)
		fmt.Println(t.Mensagem)
	}
}

func (b *db) buscaID(id int64){
	tweet := banco.Tweet{}

	err := b.tweets.BuscaPorID(id, &tweet)
	if err != nil {
		log.Printf("Falha ao buscar o ID requisitado, erro: %v", err)
	}
}

func main() {
	var id int64
	tabelaHashtags, err := banco.NovaTabelaHashtags()
	if err != nil {
		log.Fatalf("Falha ao criar tabela de hashtags, err: %v", err)
	}
	tabelaTweets, err := banco.NovaTabelaTweets(tabelaHashtags)
	if err != nil {
		log.Fatalf("Falha ao criar tabela de tweets, err: %v", err)
	}
	b := &db{
		tweets:   tabelaTweets,
		hashtags: tabelaHashtags,
	}
	for {
		var opcao int
		fmt.Println("\n\n1: Importar tweets dentro da pasta dados")
		fmt.Println("2: Hashtags mais usadas")
		fmt.Println("3: Buscar tweets com determinada hashtag")
		fmt.Println("4: Buscar tweet por ID")
		fmt.Println("5: Sair")
		fmt.Println("Escolha uma das opções:")
		fmt.Scanln(&opcao)
		switch opcao {
		case 1:
			err := b.importa("dados")
			if err != nil {
				fmt.Printf("Erro na importaçao, possivelmente o banco esteja invalido agora, err: %v", err)
				return
			}
			fmt.Println("Importacao terminou com sucesso")
		case 2:
			err := b.topHashtags()
			if err != nil {
				fmt.Println(err)
				continue
			}
		case 3:
			var texto string
			fmt.Println("Digite o texto da hashtag (sem o #) e aperte enter:")
			fmt.Scanln(&texto)
			fmt.Printf("Buscando tweets com a hashtag: %s\n", texto)
			b.tweetsPorHashtag(texto)
		case 4:
			fmt.Println("Digite o ID do Tweeter desejado e aperte enter:")
			fmt.Scanln(&id)
			fmt.Println("Buscando tweets pelo ID:", id)
			b.buscaID(id)
		case 5:
			b.hashtags.Close()
			b.tweets.Close()
			return
		}
	}

}
