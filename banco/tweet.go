package banco

import (
	"strings"
	"encoding/binary"
	"time"
	"fmt"
)

const tamanhoRegistro = 550

type Tweet struct {
	Endereco int64 // 10 bytes
	ID         int64     //10 bytes
	Nome       string    //20 bytes
	Localidade string    //20 bytes
	Data       time.Time //10 bytes
	Hashtags   []string  //200 bytes
	Mensagem   string    //280 bytes
}


func (t Tweet) Converte() []byte {
	aux := make([]byte, tamanhoRegistro)             //slice é um vetor dinânimo
	binary.PutVarint(aux[0:10], t.Endereco)                //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	binary.PutVarint(aux[10:20], t.ID)                //estou passando as posições de 0 até 10 para a variável ID da struct Tweet
	copy(aux[20:40], t.Nome)                         //[10:20] onde começa e o byte limite+1
	copy(aux[40:60], t.Localidade)                   //string's são internamente representadas por slice de bytes, sendo assim copy
	binary.PutVarint(aux[60:70], t.Data.Unix())      //retorna a data em segundos
	copy(aux[70:270], strings.Join(t.Hashtags, ",")) //retorna a string das hashtags separadas por vírgula
	copy(aux[270:550], t.Mensagem)

	return aux
}

func (t *Tweet) Desconverte(aux []byte) error { //ponteiro para poder alterar os dados
	if len(aux) != tamanhoRegistro {
		return fmt.Errorf("Tamanho Inválido, esperava %d mas recebeu %d", tamanhoRegistro, len(aux))
	}
	t.Endereco, _ = binary.Varint(aux[0:10])
	t.ID, _ = binary.Varint(aux[10:20])
	t.Nome = strings.Trim(string(aux[20:40]), "\x00")
	t.Localidade = strings.Trim(string(aux[40:60]), "\x00")
	timestamp, _ := binary.Varint(aux[60:70])
	t.Data = time.Unix(timestamp, 0)                                           //recebe os segundos e nanosegundo e retorna um time
	t.Hashtags = strings.Split(strings.Trim(string(aux[70:270]), "\x00"), ",") //separo as hashtag's pela ,
	t.Mensagem = strings.Trim(string(aux[270:550]), "\x00")

	return nil //retorna nulo
}
