console.time("tempoExecucao");
const fs = require('fs');
const csv = require('csv-parser');
const { stringify } = require('csv-stringify');

function formatarComoMoedaBRL(valor) {
  return new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
  }).format(valor);
}

const validarDigitoVerificador = (pesos,documento) =>{
  let digitoVerificador = 0;

  pesos.map((value,index)=>{
      digitoVerificador += value * parseInt(documento[index]);
  });

  digitoVerificador %= 11;

  digitoVerificador = digitoVerificador<2 ? 0 : 11 - digitoVerificador ;

  return digitoVerificador == parseInt(documento[pesos.length])
}


 function validarLinha (data) {
  nrCpfCnpj = new Number(data["nrCpfCnpj"]);

  if(/^\d{11}$/.test(nrCpfCnpj)){
    let pesosCpf = [10, 9, 8, 7, 6, 5, 4, 3, 2]

    if(!validarDigitoVerificador(pesosCpf,nrCpfCnpj)) data.validadeCpfCNPJ ="CPF inválido";

    pesosCpf.unshift(11);

    data.validadeCpfCNPJ = validarDigitoVerificador(pesosCpf,nrCpfCnpj)? "CPF válido":"CPF inválido"
  }
  else if(/^\d{14}$/.test(nrCpfCnpj)){
    let pesosCnpj = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    if(!validarDigitoVerificador(pesosCnpj,nrCpfCnpj)) data.validadeCpfCNPJ ="CPF inválido";

    pesosCnpj.unshift(6);

    data.validadeCpfCNPJ = validarDigitoVerificador(pesosCnpj,nrCpfCnpj)? "CPF válido":"CPF inválido"
  }
  else data.validadeCpfCNPJ = "número de documento inválido"


  vlTotal = new Number(data["vlTotal"]);
  qtPrestacoes = new Number(data["qtPrestacoes"]);
  vlPresta = new Number(data["vlPresta"]);

  data.valorDaPrestaçãoCorreto = vlPresta / qtPrestacoes == vlPresta ? "correto" : `errado ${vlPresta / qtPrestacoes == vlPresta}`

  data["vlTotal"] = formatarComoMoedaBRL(vlTotal);
  data["vlPresta"] = formatarComoMoedaBRL(vlPresta);
  data["vlMora"] = formatarComoMoedaBRL(new Number(data["vlMora"]));
  data["vlMulta"] = formatarComoMoedaBRL(new Number(data["vlMulta"]));
  data["vlOutAcr"] = formatarComoMoedaBRL(new Number(data["vlOutAcr"]));
  data["vlIof"] = formatarComoMoedaBRL(new Number(data["vlIof"]));
  data["vlDescon"] = formatarComoMoedaBRL(new Number(data["vlDescon"]));
  data["vlAtual"] = formatarComoMoedaBRL(new Number(data["vlAtual"]));

  return data;
}


async function parseCSV() {
  const results = [];
  
  await new Promise((resolve, reject) => {
    fs.createReadStream('data.csv')
    .pipe(csv())
    .on('data', (data) =>  results.push(data))
    .on('end', resolve)
    .on('error', reject); 
  });
  
  return results;
}
async function gerarNovoCSV(data) {
  const writableStream = fs.createWriteStream('result.csv');
  const stringifier = stringify({ header: true });

  stringifier.pipe(writableStream);

  data.forEach(row => {
    stringifier.write(row);
  });

  stringifier.end();
}
(async () => {

  const results = await parseCSV().then(data=> data.map(validarLinha));

  gerarNovoCSV(results)
  console.timeEnd("tempoExecucao"); 
})();
