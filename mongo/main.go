package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os" //para trabalhar com arquivos
	"path/filepath"
	"strings"
	"time"

	"github.com/dghubble/go-twitter/twitter"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type db struct {
	client   *mongo.Client
	hashtags *mongo.Collection
	tweets   *mongo.Collection
}

type tweet struct {
	ID         int64     `bson:"id"`
	Nome       string    `bson:"nome"`
	Localidade string    `bson:"localidade"`
	Data       time.Time `bson:"data"`
	Mensagem   string    `bson:"mensagem"`
}

type hashtag struct {
	Texto       string `bson:"texto"`
	TotalTweets int64  `bson:"total_tweets"`
}

func (b *db) importa(diretorio string) error {
	// https://progolang.com/listing-files-and-directories-in-go/
	//retorna em arquivos todos aquele que encontrou
	arquivos, err := filepath.Glob(filepath.Join(diretorio, "*.data"))
	if err != nil {
		return err
	}
	ctx := context.Background()
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

			hashtags := []hashtag{}
			for _, h := range t.Entities.Hashtags {
				hashtag := hashtag{}
				texto := strings.ToLower(h.Text)
				//retorna apenas um único documento filtrado pelo 2° parametro
				err := b.hashtags.FindOne(ctx, bson.D{{"texto", texto}}).Decode(&hashtag)
				if err != nil && err != mongo.ErrNoDocuments {
					log.Fatalf("Erro desconhecido ao buscar hashtag, err: %v", err)
				}

				if err == mongo.ErrNoDocuments {
					// Se não encontrar a hashtag cadastramos ela
					hashtag.Texto = texto
					//insere um único documento na colection
					res, err := b.hashtags.InsertOne(ctx, hashtag)
					if err != nil {
						log.Fatalf("Erro desconhecido ao salvar hashtag, err: %v", err)
					}
					fmt.Printf("Inseriu documento com ID %v\n", res.InsertedID)
				}
				hashtags = append(hashtags, hashtag)
			}

			registro := tweet{
				ID:         t.ID,
				Nome:       t.User.ScreenName,
				Localidade: t.User.Location,
				Data:       data,
				Mensagem:   t.Text,
			}
			res, err := b.tweets.InsertOne(ctx, registro)
			if err != nil {
				log.Fatalf("Erro desconhecido ao salvar o tweet, err: %v", err)
			}
			fmt.Printf("Inseriu documento com ID %v\n", res.InsertedID)
		}
	}
	return nil
}

func (b *db) topHashtags() error {
	return nil
}

func (b *db) tweetsPorHashtag(texto string) {

}

func (b *db) buscaID(id int64) {
}

func main() {
	//https://github.com/mongodb/mongo-go-driver
	//Cria o client para depois se conectar
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal("Falha ao se conectar com o banco:", err)
	}
	//Conecta com o banco
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal("Falha ao se conectar com o banco:", err)
	}
	b := &db{
		client:   client,
		hashtags: client.Database("trabalho").Collection("hashtags"),
		tweets:   client.Database("trabalho").Collection("tweets"),
	}

	for {
		var opcao, id int64
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
			return
		}
	}

}
