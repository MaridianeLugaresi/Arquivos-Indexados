package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os" //para trabalhar com arquivos
	"path/filepath"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/maridianelugaresi/arquivos-indexados/banco"
)

func importa(diretorio string) error {
	tabelaTweets, err := banco.NovaTabela("tweets")
	if err != nil {
		return err
	}

	// https://progolang.com/listing-files-and-directories-in-go/
	arquivos, err := filepath.Glob(filepath.Join(diretorio, "*.data")) //retorna em arquivos todos aquele que encontrou
	if err != nil {
		return err
	}
	for _, arquivo := range arquivos { //faz o for passando cada arquivo de arquivos
		//Abrindo o arquivo
		f, err := os.Open(arquivo)
		if err != nil { //nil é nulo
			return err
		}

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

			hashtags := []string{} //criação sem dados

			for _, h := range t.Entities.Hashtags { //esta retornando o indice(_), e a hashtag
				hashtags = append(hashtags, h.Text)
			}

			registro := tweet{
				ID:         t.ID,
				nome:       t.User.ScreenName,
				localidade: t.User.Location,
				data:       data,
				hashtags:   hashtags,
				mensagem:   t.Text,
			}

			err = tabelaTweets.Inserir(&registro)
			if err != nil {
				log.Printf("Erro ao salvar Tweet: %v", err)
			}
		}
	}

	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func main() {

	importa("dados")

}
