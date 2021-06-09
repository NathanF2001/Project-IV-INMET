const AWS = require("aws-sdk");
const s3 = new AWS.S3({ apiVersion: '2021-06-01' });

exports.handler = async (event, context, callback) => {

  let result;
  // Checa se api gateway mandou algum query params
  if (event["queryStringParameters"] !== null) {
    const data = event["queryStringParameters"]["data"];
    const [year, month, day] = data.split("-").map(element => parseInt(element));

    if (year.isNaN || month === undefined || day === undefined) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          status: 400,
          mensage: "Parâmetro errado, utilize o formato YYYY-MM-DD"
        })
      };
    } else {

      result = await s3.getObject({
        Bucket: 'inmet-data',
        Key: `results/${year}/${month - 1}/${day}/log_day.json`
      }
      ).promise().catch(error => {
        return error;
      });

      if (result.code === "NoSuchKey") {
        return {
          statusCode: 400,
          body: JSON.stringify({
            status: 400,
            mensage: "Data Inválida, escolha um dia anterior ao dia atual"
          })
        }
      }
    }
  } else {
    result = await s3.getObject({
      Bucket: 'inmet-data',
      Key: 'result_today.json'
    }
    ).promise();
  }

  result = result.Body.toString('utf8');

  return {
    "statusCode": 200,
    "headers": { "Content-Type": "application/json" },
    "body": result
  }

};
