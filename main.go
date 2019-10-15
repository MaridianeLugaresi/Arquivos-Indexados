//próximo passo é fazer a escrita no arquivo
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os" //para trabalhar com arquivos
	"path/filepath"
	"strings"
	"time"

	"github.com/dghubble/go-twitter/twitter"
)

const tamanhoRegistro = 550
const tamanhoRegistroIndice = 20

//https://golang.org/pkg/os/#File.Seek
const inicioArquivo = 0
const finalArquivo = 2

type tweet struct {
	endereco int64 // 10 bytes
	ID         int64     //10 bytes
	nome       string    //20 bytes
	localidade string    //20 bytes
	data       time.Time //10 bytes
	hashtags   []string  //200 bytes
	mensagem   string    //280 bytes
}

type banco struct {
	arquivoSequencialEscrita *os.File
	indicePrimarioEscrita    *os.File
	
	arquivoSequencialLeitura *os.File
	indicePrimarioLeitura    *os.File
}

type indice struct {
	chave   int64
	posicao int64
}

func novoBanco() (*banco, error) {
	dbEscrita, err := os.OpenFile("db.bin", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	indiceEscrita, err := os.OpenFile("indice-primario.bin", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}

	dbLeitura, err := os.Open("db.bin")
	if err != nil {
		return nil, err
	}

	indiceLeitura, err := os.Open("indice-primario.bin")
	if err != nil {
		return nil, err
	}

	return &banco{
		arquivoSequencialEscrita: dbEscrita,
		indicePrimarioEscrita: indiceEscrita,
		arquivoSequencialLeitura: dbLeitura,
		indicePrimarioLeitura: indiceLeitura,
	}, nil
}

func (b *banco) importa(diretorio string) error {
	//Busca o ultimo registro inserido para pegar o ultimo endereco utilizado
	_, err := b.arquivoSequencialLeitura.Seek(tamanhoRegistro, finalArquivo)
	if err != nil {
		return err
	}
	registro := &tweet{}
	aux := make([]byte, tamanhoRegistro)
	bytesLidos, err := b.arquivoSequencialLeitura.Read(aux)
	if bytesLidos != tamanhoRegistro || err != nil {
		return fmt.Errorf("esperando ler %d bytes mas leu apenas %d, err: %v", bytesLidos, tamanhoRegistro, err)
	}
	err = registro.desconverte(aux)
	if err != nil {
		return err
	}
	endereco := registro.endereco


	// https://progolang.com/listing-files-and-directories-in-go/
	arquivos, err := filepath.Glob(filepath.Join(diretorio, "*.data")) //retorna em arquivos todos aquele que encontrou
	if err != nil {
		return err
	}
	for _, arquivo := range arquivos { //faz o for passando cada arquivo de arquivos
		//Abrindo o arquivo
		f, err := os.Open(arquivo)
		if err != nil { //nil é nulo
			log.Fatal(err)
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
			registroExiste := b.buscaPelaChavePrimaria(registro.ID)
			if registroExiste != nil {
				log.Printf("registro %d já existe no banco", registro.ID)
				continue
			}
			endereco++
			registro.endereco = endereco
			_, err := b.arquivoSequencialEscrita.Write(registro.converte()) //escreve no arquivo db utilizando a função criada
			if err != nil {
				log.Fatal(err)
			}
			ind := indice{
				chave: registro.ID,
				posicao: registro.endereco,
			}
			_, err := b.indicePrimarioEscrita.Write(ind.converte()) //escreve no arquivo de indice 
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func (b *banco) buscaPelaChavePrimaria(id int64) (*tweet) {
	return nil
}

func menu_principal() {

}

func (b *banco) fechaArquivos() {

	if err := b.arquivo.Close(); err != nil {
		log.Fatal(err)
	}
}

func (t tweet) converte() []byte {
	aux := make([]byte, tamanhoRegistro)             //slice é um vetor dinânimo
	binary.PutVarint(aux[0:10], t.endereco)                //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	binary.PutVarint(aux[10:20], t.ID)                //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	copy(aux[20:40], t.nome)                         //[10:20] onde começa e o byte limite+1
	copy(aux[40:60], t.localidade)                   //string's são internamente representadas por slice de bytes, sendo assim copy
	binary.PutVarint(aux[60:70], t.data.Unix())      //retorna a data em segundos
	copy(aux[70:270], strings.Join(t.hashtags, ",")) //retorna a string das hashtags separadas por vírgula
	copy(aux[270:550], t.mensagem)

	return aux
}

func (t *tweet) desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoRegistro {
		return fmt.Errorf("Tamanho Inválido, esperava %d mas recebeu %d", tamanhoRegistro, len(aux))
	}
	t.endereco, _ = binary.Varint(aux[0:10])
	t.ID, _ = binary.Varint(aux[10:20])
	t.nome = strings.Trim(string(aux[20:40]), "\x00")
	t.localidade = strings.Trim(string(aux[40:60]), "\x00")
	timestamp, _ := binary.Varint(aux[60:70])
	t.data = time.Unix(timestamp, 0)                                           //recebe os segundos e nanosegundo e retorna um time
	t.hashtags = strings.Split(strings.Trim(string(aux[70:270]), "\x00"), ",") //separo as hashtag's pela ,
	t.mensagem = strings.Trim(string(aux[270:550]), "\x00")

	return nil //retorna nulo
}

func (i indice) converte() []byte {
	aux := make([]byte, tamanhoRegistroIndice)
	binary.PutVarint(aux[0:10], i.chave)
	binary.PutVarint(aux[10:20], i.posicao)
}

func (i *indice) desconverte (aux []byte) error {
	if len(aux) != tamanhoRegistroIndice {
		return fmt.Errorf("Tamanho Inválido, esperava %d mas recebeu %d", tamanhoRegistroIndice, len(aux))
	}
	i.chave, _ := binary.Varint(aux[0:10])
	i.posicao, _ := binary.Varint(aux[10:20])
	return nil
}

func main() {

	db, err := novoBanco()

	if err != nil {
		log.Fatal(err)
	}

	db.importa()

}
