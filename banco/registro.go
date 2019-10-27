package banco

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

const tamanhoTweet = 340
const tamanhoHashtag = 110

type Tweet struct {
	ID         int64     //10 bytes
	Nome       string    //20 bytes
	Localidade string    //20 bytes
	Data       time.Time //10 bytes
	Hashtags   []Hashtag //registros associados com o tweet
	Mensagem   string    //280 bytes
}

type Hashtag struct {
	ID    int64  // 10 bytes
	Texto string // 100 bytes
	// Campos ignorados no banco
	TotalTweets int64
}

func (t Tweet) Converte() []byte {
	aux := make([]byte, tamanhoTweet)			//alocando uma área que pode armazenar um Tweet
	binary.PutVarint(aux[0:10], t.ID)           //escreve a variável ID
	copy(aux[10:30], t.Nome)             		//escreve a variável Nome
	copy(aux[30:50], t.Localidade)              //escreve a localidade
	binary.PutVarint(aux[50:60], t.Data.Unix()) //retorna a data em segundos
	copy(aux[60:340], t.Mensagem)				//escreve a mensagem

	return aux
}

func (t *Tweet) Desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoTweet {
		return fmt.Errorf("Tamanho inválido, esperava %d mas recebeu %d", tamanhoTweet, len(aux))
	}
	t.ID, _ = binary.Varint(aux[0:10])
	t.Nome = strings.Trim(string(aux[10:30]), "\x00")
	t.Localidade = strings.Trim(string(aux[30:50]), "\x00")
	timestamp, _ := binary.Varint(aux[50:60])
	t.Data = time.Unix(timestamp, 0) //recebe os segundos e nanosegundo e retorna um time
	t.Mensagem = strings.Trim(string(aux[60:340]), "\x00")

	return nil //retorna nulo
}

func (t *Tweet) IDRegistro() int64 {
	return t.ID
}

func (h Hashtag) Converte() []byte {
	aux := make([]byte, tamanhoHashtag) 
	binary.PutVarint(aux[0:10], h.ID)   //converte o ID e escreve em aux
	copy(aux[10:110], h.Texto)          //escreve o texto das hashtags
	return aux
}

func (h *Hashtag) Desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoHashtag {
		return fmt.Errorf("Tamanho inválido, esperava %d mas recebeu %d", tamanhoHashtag, len(aux))
	}
	h.ID, _ = binary.Varint(aux[0:10])
	h.Texto = strings.Trim(string(aux[10:110]), "\x00")
	return nil
}

func (h Hashtag) IDRegistro() int64 {
	return h.ID
}

func (h *Hashtag) CadastraIDRegistro(id int64) {
	h.ID = id
}
