const AWS = require("aws-sdk");
const kinesis = new AWS.Kinesis();
const s3 = new AWS.S3({ apiVersion: '2021-06-01' });

// Algumas saída de varchar contêm espaços em branco, essa função retira esses espaços
String.prototype.trim = function () {
  return String(this).replace(/^\s+|\s+$/g, '');
};

exports.handler = async (event, context) => {

  // Pega o nome e o bucket do objeto que foi adicionado no primeiro bucket
  const bucket_name_kinesis = event.Records[0].s3.bucket.name;
  const key_kinesis = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));

  // Processamento do dado no S3
  let kinesis_output = await s3.getObject({
    Bucket: bucket_name_kinesis,
    Key: key_kinesis
  }).promise();
  kinesis_output = kinesis_output.Body.toString("utf8").replace(/}{/g, "}\n{")
  kinesis_output = kinesis_output.split("\n")
  // O dado gerando pelo kinesis analytics
  const kinesis_processed = kinesis_output.map(element => JSON.parse(element));


  // Pegando os dados do reference data
  let destination = await s3.getObject({
    Bucket: 'inmet-data',
    Key: 'result_today.json'
  }
  ).promise();
  destination = JSON.parse(destination.Body.toString('utf8'));

  let data_finded = false;
  let end_day = false;

  if (kinesis_processed[0].HORARIO == 23) {
    end_day = true;
  }

  // Atualizando o reference data
  kinesis_processed.forEach(data => {
    data.NOME_ESTACAO = data.NOME_ESTACAO.trim();
    data.CODIGO_ESTACAO = data.CODIGO_ESTACAO.trim();
    data.TIPO_DADO = data.TIPO_DADO.trim();

    data_finded = false;
    for (let i = 0; i <= destination.length; i++) {
      if ((destination[i] != undefined) && (destination[i].TIPO_DADO == data.TIPO_DADO)) {

        data_finded = true;
        destination[i] = data;
        break;
      }
    }
    if (!data_finded) {
      destination.push(data);
    }
  });


  // Condição caso for fim do dia, adicionar para o outro bucket e reseta os valores do dia para default
  if (end_day) {
    const time = new Date();
    await s3.putObject({
      Bucket: 'inmet-data',
      Key: `results/${time.getFullYear()}/${time.getMonth()}/${time.getDate()}/log_day.json`,
      Body: JSON.stringify(destination),
      ContentType: "application/json"
    }
    ).promise();

    await s3.putObject({
      Bucket: 'inmet-data',
      Key: 'result_today.json',
      Body: JSON.stringify([
        { TIPO_DADO: "TEMP_AVG", VALUE_CAPTURED: 0 },
        { TIPO_DADO: "UMID_AVG", VALUE_CAPTURED: 0 },
        { TIPO_DADO: "CHUVAS", VALUE_CAPTURED: 0 },
        { TIPO_DADO: "UMID_MAX", VALUE_CAPTURED: 0 },
        { TIPO_DADO: "UMID_MIN", VALUE_CAPTURED: 300 },
        { TIPO_DADO: "TEMP_MAX", VALUE_CAPTURED: -255 },
        { TIPO_DADO: "TEMP_MIN", VALUE_CAPTURED: 100 },
      ]),
      ContentType: "application/json"
    }
    ).promise();

  } else {
    const result = await s3.putObject({
      Bucket: 'inmet-data',
      Key: 'result_today.json',
      Body: JSON.stringify(destination),
      ContentType: "application/json"
    }
    ).promise();
  }



  return { mensage: "Os dados foram atualizados" };
};
