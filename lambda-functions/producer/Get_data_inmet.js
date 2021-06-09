const axios = require("axios");
const AWS = require("aws-sdk");
const kinesis = new AWS.Kinesis();

module.exports.handle = async (event) => {

  const time = new Date();
  const actual_hour = time.getUTCHours();
  //Os dados da api está em UTC
  const actual_day = `${time.getUTCFullYear()}-${time.getUTCMonth() + 1}-${time.getUTCDate()}`;

  const url = `https://apitempo.inmet.gov.br/estacao/dados/${actual_day}/${("0" + actual_hour).slice(-2)}00/`;

  const response = await axios.get(url);
  // lista de todos dados meteorológicos de todos estado no horario definido
  const list_data = response.data;

  // Filtrar para dados de pernambuco
  const data_PE = list_data.filter(estacao => estacao.UF == "PE");


  // Pré processamento de dados que serão utilizados
  const records = data_PE.map(element => {
    element["HORARIO"] = ((((actual_hour - 3) % 24) + 24) % 24);
    const data = {
      "DC_NOME": element["DC_NOME"],
      "VL_LATITUDE": element["VL_LATITUDE"],
      "VL_LONGITUDE": element["VL_LONGITUDE"],
      "TEM_MAX": element["TEM_MAX"] == null ? 0 : element["TEM_MAX"],
      "TEM_MIN": element["TEM_MIN"] == null ? 1000 : element["TEM_MIN"],
      "TEM_INS": element["TEM_INS"],
      "UMD_MAX": element["UMD_MAX"] == null ? 0 : element["UMD_MAX"],
      "UMD_MIN": element["UMD_MIN"] == null ? 1000 : element["UMD_MIN"],
      "UMD_INS": element["UMD_INS"],
      "CD_ESTACAO": element["CD_ESTACAO"],
      "CHUVA": element["CHUVA"],
      "HORARIO": element["HORARIO"]
    }
    return {
      "Data": JSON.stringify(data),
      "PartitionKey": `key_1`
    };
  });

  // Enviando os dados para kinesis
  return await kinesis.putRecords({
    "Records": records,
    "StreamName": "receive-lambda-INMET"
  }).promise();
};
