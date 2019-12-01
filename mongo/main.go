package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dghubble/go-twitter/twitter"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/kevinburke/nacl"
	"github.com/kevinburke/nacl/secretbox"
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
	Hashtags   []string  `bson:"hashtags"`
	Mensagem   string    `bson:"mensagem"`
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

			hashtags := make([]string, len(t.Entities.Hashtags))
			for i, h := range t.Entities.Hashtags {
				hashtags[i] = strings.ToLower(h.Text)
			}

			registro := tweet{
				ID:         t.ID,
				Nome:       t.User.ScreenName,
				Localidade: t.User.Location,
				Data:       data,
				Hashtags:   hashtags,
				Mensagem:   t.Text,
			}
			key, err := nacl.Load("6368616e676520746869732070617373776f726420746f206120736563726574")
			if err != nil {
				log.Fatal("Erro ao criar chave criptografica, err:", err)
			}
			encrypted := secretbox.EasySeal([]byte(registro.Nome), key)
			fmt.Println(base64.StdEncoding.EncodeToString(encrypted))
			registro.Nome = string(encrypted)

			//insere o registro no banco
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
	ctx := context.Background()
	query := mongo.Pipeline{
		{{"$unwind", "$hashtags"}},
		{{"$group", bson.D{
			{"_id", "$hashtags"},
			{"count", bson.D{
				{"$sum", 1},
			}},
		}}},
		{{"$sort", bson.D{
			{"count", -1},
		}}},
	}
	cursor, err := b.tweets.Aggregate(context.Background(), query)
	if err != nil {
		log.Fatal("Erro ao agregar hashtags, err: ", err)
	}

	defer cursor.Close(ctx)

	i := 0
	fmt.Printf("Posicao | TotalTweets | Texto\n")
	for cursor.Next(ctx) && i <= 500 {
		i++
		resultado := map[string]interface{}{}
		err := cursor.Decode(&resultado)
		if err != nil {
			log.Fatal("Erro ao decodificar, err:", err)
		}

		fmt.Printf("%d | %d | %s\n", i, resultado["count"], resultado["_id"])
	}
	return nil
}

func (b *db) tweetsPorHashtag(texto string) {
	ctx := context.Background()
	cursor, err := b.tweets.Find(context.Background(),
		bson.D{
			{"hashtags", texto},
		})
	if err != nil {
		log.Fatal("Erro ao buscar tweets por hashtags, err: ", err)
	}

	defer cursor.Close(ctx)

	i := 0
	t := tweet{}
	fmt.Printf("Posicao | TotalTweets | Texto\n")
	for cursor.Next(ctx) {
		err := cursor.Decode(&t)
		if err != nil {
			log.Fatal("Erro ao decodificar tweet, err:", err)
		}
		i++
		hashtags := ""
		for _, hashtag := range t.Hashtags {
			hashtags += "[#" + hashtag + "]"
		}
		key, err := nacl.Load("6368616e676520746869732070617373776f726420746f206120736563726574")
		if err != nil {
			log.Fatal("Erro ao criar chave criptografica, err:", err)
		}
		nome, err := secretbox.EasyOpen([]byte(t.Nome), key)
		if err != nil {
			log.Fatal("Erro a descriptografar tweet, err:", err)
		}
		t.Nome = string(nome)
		fmt.Printf("\nID: %d Nome: %s Data: %s Localidade: %s\n", t.ID, t.Nome, t.Data.Format(time.RFC3339), t.Localidade)
		fmt.Printf("Hashtags: %s\n", hashtags)
		fmt.Println(t.Mensagem)
	}
}

func (b *db) buscaID(id int64) {
	ctx := context.Background()
	cursor, err := b.tweets.Find(context.Background(),
		bson.D{
			{"id", id},
		})
	if err != nil {
		log.Fatal("Erro ao buscar tweets por ID, err: ", err)
	}

	defer cursor.Close(ctx)

	i := 0
	tweet := tweet{}
	fmt.Printf("Tweet:\n")
	for cursor.Next(ctx) {
		err := cursor.Decode(&tweet)
		if err != nil {
			log.Fatal("Erro ao decodificar tweet, err:", err)
		}
		i++
		hashtags := ""
		for _, hashtag := range tweet.Hashtags {
			hashtags += "[#" + hashtag + "]"
		}
		key, err := nacl.Load("6368616e676520746869732070617373776f726420746f206120736563726574")
		if err != nil {
			log.Fatal("Erro ao criar chave criptografica, err:", err)
		}
		nome, err := secretbox.EasyOpen([]byte(tweet.Nome), key)
		if err != nil {
			log.Fatal("Erro a descriptografar tweet, err:", err)
		}
		tweet.Nome = string(nome)
		fmt.Printf("\nID: %d Nome: %s Data: %s Localidade: %s\n", tweet.ID, tweet.Nome, tweet.Data.Format(time.RFC3339), tweet.Localidade)
		fmt.Printf("Hashtags: %s \n", tweet.Hashtags)
		fmt.Printf(tweet.Mensagem)
	}
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
