package banco

import "os"

type Tabela struct {
	nome            string
	tamanhoRegistro int64
	proximoEndereco int64

	file           *os.File
	indicePrimario *IndicePrimario
}

type Registro interface {
	Converte() []byte
	Desconverte(aux []byte) error
	RegistroID() int64
}

func NovaTabela(nome string, tamanhoRegistro int64) (*Tabela, error) {
	file, err := os.OpenFile(nome+"-tabela.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	indice, err := NovoIndicePrimario(nome)
	if err != nil {
		return nil, err
	}
	return &Tabela{
		nome:            nome,
		file:            file,
		indicePrimario:  indice,
		tamanhoRegistro: tamanhoRegistro,
	}, nil
}

func (t *Tabela) Inserir(registro Registro) error {
	t.file.Seek(0, finalArquivo)
	_, err := t.file.Write(registro.Converte()) //escreve no arquivo db utilizando a função criada
	if err != nil {
		return err
	}
	return t.indicePrimario.Inserir(registro.RegistroID(), t.proximoEndereco)
}

func (t *Tabela) BuscaPorID(id int64, registro Registro) error {
	endereco, err := t.indicePrimario.Busca(id)
	if err != nil {
		return err
	}
	return t.BuscarPorEndereco(endereco, registro)
}

func (t *Tabela) BuscarPorEndereco(endereco int64, registro Registro) error {
	t.file.Seek(endereco*t.tamanhoRegistro, inicioArquivo)
	aux := make([]byte, t.tamanhoRegistro)
	_, err := t.file.Read(aux)
	if err != nil {
		return err
	}
	return registro.Desconverte(aux)
}
