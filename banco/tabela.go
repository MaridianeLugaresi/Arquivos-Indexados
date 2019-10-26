package banco

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

type TabelaTweets struct {
	tamanhoRegistro int64
	proximoEndereco int64

	file           *os.File
	indicePrimario *IndicePrimario
	tabelaHashtags *TabelaHashtags
}

type TabelaHashtags struct {
	tamanhoRegistro int64
	proximoEndereco int64

	file               *os.File
	indicePrimario     *IndicePrimario
	indiceTexto        *IndiceSecundarioString
	indiceTweetHashtag *IndiceSecundarioNeN
}

func NovaTabelaTweets(tabelaHashtags *TabelaHashtags) (*TabelaTweets, error) {
	file, err := os.OpenFile("tweets-tabela.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	indice, err := NovoIndicePrimario("tweets")
	if err != nil {
		return nil, err
	}
	return &TabelaTweets{
		file:            file,
		indicePrimario:  indice,
		tamanhoRegistro: tamanhoTweet,
		proximoEndereco: stats.Size() / tamanhoTweet,
		tabelaHashtags:  tabelaHashtags,
	}, nil
}

func (t *TabelaTweets) Inserir(tweet Tweet) error {
	t.file.Seek(0, finalArquivo)
	err := t.indicePrimario.Inserir(tweet.ID, t.proximoEndereco)
	if err != nil {
		return err
	}
	_, err = t.file.Write(tweet.Converte()) //escreve no arquivo da tabela utilizando a função criada
	if err != nil {
		return err
	}
	err = t.tabelaHashtags.VincularTweet(tweet)
	if err != nil {
		return err
	}
	t.proximoEndereco++
	return nil
}

func (t *TabelaTweets) BuscaPorHashtag(hashtagID int64) ([]Tweet, error) {
	ids := t.tabelaHashtags.TweetsIDsComHashtag(hashtagID)
	tweets := make([]Tweet, 0, len(ids))
	for _, id := range ids {
		tweet := Tweet{}
		err := t.BuscaPorID(id, &tweet)
		if err != nil {
			log.Printf("Falha ao buscar Tweet com ID: %d com a hashtagID: %d, err: %v", id, hashtagID, err)
			continue
		}
		tweets = append(tweets, tweet)
	}
	return tweets, nil
}

func (t *TabelaTweets) BuscaPorID(id int64, tweet *Tweet) error {
	endereco, err := t.indicePrimario.Busca(id)
	if err != nil {
		return err
	}
	return t.BuscaPorEndereco(endereco, tweet)
}

func (t *TabelaTweets) BuscaPorEndereco(endereco int64, tweet *Tweet) error {
	t.file.Seek(endereco*t.tamanhoRegistro, inicioArquivo)
	aux := make([]byte, t.tamanhoRegistro)
	n, err := t.file.Read(aux)
	if err != nil && n != int(t.tamanhoRegistro) {
		return fmt.Errorf("falha ao buscar o tweet no endereco %d, err: %v", endereco, err)
	}
	err = tweet.Desconverte(aux)
	if err != nil {
		return fmt.Errorf("falha ao desconverter o tweet, err: %v", err)
	}
	tweet.Hashtags, err = t.tabelaHashtags.BuscaHashtagsPorTweet(tweet.ID)
	return err
}

func (t *TabelaTweets) Close() {
	err := t.indicePrimario.Close()
	if err != nil {
		log.Printf("erro fechando indice primario, err: %v", err)
	}
	err = t.file.Close()
	if err != nil {
		log.Printf("erro fechando arquivo de dados, err: %v", err)
	}
}

func NovaTabelaHashtags() (*TabelaHashtags, error) {
	file, err := os.OpenFile("hashtags-tabela.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	indice, err := NovoIndicePrimario("hashtags")
	if err != nil {
		return nil, err
	}
	indiceTexto, err := NovoIndiceSecundarioString("hashtag-texto", 100, true)
	if err != nil {
		return nil, err
	}
	indiceHashtags, err := NovoIndiceSecundarioNeN("tweets-hashtags")
	if err != nil {
		return nil, err
	}
	return &TabelaHashtags{
		file:               file,
		indicePrimario:     indice,
		indiceTexto:        indiceTexto,
		indiceTweetHashtag: indiceHashtags,
		tamanhoRegistro:    tamanhoHashtag,
		proximoEndereco:    stats.Size() / tamanhoHashtag,
	}, nil
}

func (t *TabelaHashtags) Inserir(hashtag *Hashtag) error {
	t.file.Seek(0, finalArquivo)
	err := t.indiceTexto.Inserir(hashtag.Texto, t.proximoEndereco)
	if err != nil {
		return err
	}
	hashtag.ID = t.proximoEndereco + 1        //Evitamos o ID 0
	_, err = t.file.Write(hashtag.Converte()) //escreve no arquivo da tabela utilizando a função criada
	if err != nil {
		return err
	}
	err = t.indicePrimario.Inserir(hashtag.ID, t.proximoEndereco)
	if err != nil {
		return err
	}
	t.proximoEndereco++
	return nil
}

func (t *TabelaHashtags) VincularTweet(tweet Tweet) error {
	for _, hashtag := range tweet.Hashtags {
		err := t.indiceTweetHashtag.Inserir(tweet.ID, hashtag.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TabelaHashtags) BuscaPorTexto(texto string, hashtag *Hashtag) error {
	enderecos, err := t.indiceTexto.BuscaPorValor(texto)
	if err != nil {
		return err
	}
	return t.BuscaPorEndereco(enderecos[0], hashtag)
}

func (t *TabelaHashtags) BuscaPorID(id int64, hashtag *Hashtag) error {
	endereco, err := t.indicePrimario.Busca(id)
	if err != nil {
		return err
	}
	return t.BuscaPorEndereco(endereco, hashtag)
}

func (t *TabelaHashtags) BuscaPorEndereco(endereco int64, hashtag *Hashtag) error {
	t.file.Seek(endereco*t.tamanhoRegistro, inicioArquivo)
	aux := make([]byte, t.tamanhoRegistro)
	n, err := t.file.Read(aux)
	if err != nil && n != int(t.tamanhoRegistro) {
		return fmt.Errorf("falha ao buscar a hashtag no endereco %d, err: %v", endereco, err)
	}
	return hashtag.Desconverte(aux)
}

func (t *TabelaHashtags) BuscaHashtagsPorTweet(tweetID int64) ([]Hashtag, error) {
	hashtagIDs, err := t.indiceTweetHashtag.BuscaPorIDPrimario(tweetID)
	if err != nil {
		return []Hashtag{}, fmt.Errorf("falha ao buscar hashtags do tweet, err: %v", err)
	}
	// Buscamos as hashtags que estão na tabela de hashtags
	hashtags := make([]Hashtag, len(hashtagIDs))
	for i, id := range hashtagIDs {
		err = t.BuscaPorID(id, &hashtags[i])
		if err != nil {
			log.Printf("falha ao buscar hashtags com o ID %d, err: %v", id, err)
		}
	}
	return hashtags, nil
}

func (t *TabelaHashtags) ListaHashtagsComCounts() (map[int64]*Hashtag, error) {
	log.Print("Buscando todas as hashtags (busca sequencial)")
	hashtags := map[int64]*Hashtag{}
	aux := make([]byte, tamanhoHashtag)
	t.file.Seek(0, inicioArquivo)
	for {
		_, err := t.file.Read(aux)
		if err == io.EOF {
			break
		}
		if err != nil {
			return hashtags, err
		}
		hashtag := &Hashtag{}
		err = hashtag.Desconverte(aux)
		if err != nil {
			return hashtags, err
		}
		hashtags[hashtag.ID] = hashtag
	}
	log.Print("Termino busca de hashtags")
	log.Print("Iniciando count de tweets")
	aux = make([]byte, tamanhoIndice)
	t.indiceTweetHashtag.file.Seek(0, inicioArquivo) // iniciamos no inicio do indice n-n
	for {
		n, err := t.indiceTweetHashtag.file.Read(aux)
		if err == io.EOF {
			break
		}
		if err != nil && n != len(aux) {
			log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
		}
		hashtagID, _ := binary.Varint(aux[10:20])
		hashtags[hashtagID].TotalTweets++
	}

	return hashtags, nil
}

func (t *TabelaHashtags) TweetsIDsComHashtag(hashtagID int64) []int64 {
	log.Printf("Buscando tweetsIDs vinculados com hashtag %d", hashtagID)
	ids := []int64{}
	aux := make([]byte, tamanhoIndice)
	t.indiceTweetHashtag.file.Seek(0, inicioArquivo) // iniciamos no inicio do indice n-n
	for {
		n, err := t.indiceTweetHashtag.file.Read(aux)
		if err == io.EOF {
			break
		}
		if err != nil && n != len(aux) {
			log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
		}
		hID, _ := binary.Varint(aux[10:20])
		if hashtagID == hID {
			tweetID, _ := binary.Varint(aux[0:10])
			ids = append(ids, tweetID)
		}
	}

	return ids
}

func (t *TabelaHashtags) Close() {
	err := t.indicePrimario.Close()
	if err != nil {
		log.Printf("erro fechando indice primario, err: %v", err)
	}
	err = t.indiceTexto.Close()
	if err != nil {
		log.Printf("erro fechando indice hashtag, err: %v", err)
	}
	err = t.indiceTweetHashtag.Close()
	if err != nil {
		log.Printf("erro fechando indice hashtag, err: %v", err)
	}
	err = t.file.Close()
	if err != nil {
		log.Printf("erro fechando arquivo de dados, err: %v", err)
	}
}
