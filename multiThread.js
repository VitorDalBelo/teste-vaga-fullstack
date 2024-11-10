console.time("tempoExecucao");
const fs = require('fs');
const csv = require('csv-parser');
const { stringify } = require('csv-stringify');
const si = require('systeminformation');

const path = require('path');
const { Worker} = require('worker_threads');

async function parseCSV() {
  const results = [];
  const csvFilePath = 'data.csv';

  await new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', resolve)
      .on('error', reject); 
  });

  return results;
}

async function processar(dadosDoArquivo) {
    const cores = await si.cpu();
  return await  new Promise((resolve, reject) => {
    const numThreads = cores.physicalCores; 
    const resultados = {
      qtdPartes: numThreads,
      qtdPartesErro: 0,
      qtsPartesSucesso: 0,
      qtsPartesProcessadas: 0,
      dados: []
    };

    const qtRegistros = dadosDoArquivo.length;
    if (qtRegistros > 0) {
      const qtPorThread = Math.ceil(qtRegistros / numThreads);
      for (let i = 0; i < numThreads; i++) {
        const start = i * qtPorThread;
        const end = start + qtPorThread;
        const parte = dadosDoArquivo.slice(start, end);

        const worker = new Worker(path.resolve(__dirname, 'worker.js'), {
          workerData: { parte, i }
        });

        worker.on('message', (data) => {
            resultados.qtsPartesSucesso++;
            resultados.qtsPartesProcessadas++;
          resultados.dados= resultados.dados.concat(data);
          if(resultados.qtsPartesProcessadas == numThreads) resolve(resultados.dados)
        });

        worker.on('error', (error) => {
          resultados.qtdPartesErro++;
          resultados.qtsPartesProcessadas++;
          console.error(`Erro no worker ${i}:`, error);
        });

        worker.on('exit', (code) => {
          if (code !== 0) {
            resultados.qtdPartesErro++;
            console.error(`Worker ${i} encerrou com erro de código ${code}`);
          }
        });
      }
    } else {
      resolve(resultados);
    }
  });
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
  const dadosDoArquivo = await parseCSV();
  const resultado = await processar(dadosDoArquivo)
  await gerarNovoCSV(resultado);
    console.timeEnd("tempoExecucao");

})();
