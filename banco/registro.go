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
	Hashtags   []Hashtag // registros associados com o tweet
	Mensagem   string    //280 bytes
}

type Hashtag struct {
	ID    int64  // 10 bytes
	Texto string // 100 bytes
}

func (t Tweet) Converte() []byte {
	aux := make([]byte, tamanhoTweet)           //slice é um vetor dinânimo
	binary.PutVarint(aux[0:10], t.ID)           //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	copy(aux[10:30], t.Nome)                    //[10:30] onde começa e o byte limite+1
	copy(aux[30:50], t.Localidade)              //string's são internamente representadas por slice de bytes, sendo assim copy
	binary.PutVarint(aux[50:60], t.Data.Unix()) //retorna a data em segundos
	copy(aux[60:340], t.Mensagem)

	return aux
}

func (t *Tweet) Desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoTweet {
		return fmt.Errorf("Tamanho Inválido, esperava %d mas recebeu %d", tamanhoTweet, len(aux))
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
	aux := make([]byte, tamanhoHashtag) //slice é um vetor dinânimo
	binary.PutVarint(aux[0:10], h.ID)   //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	copy(aux[10:110], h.Texto)          //[10:30] onde começa e o byte limite+1
	return aux
}

func (h *Hashtag) Desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoHashtag {
		return fmt.Errorf("Tamanho Inválido, esperava %d mas recebeu %d", tamanhoHashtag, len(aux))
	}
	h.ID, _ = binary.Varint(aux[0:10])
	h.Texto = strings.Trim(string(aux[10:110]), "\x00")
	return nil
}

func (h Hashtag) IDRegistro() int64 {
	return h.ID
}
