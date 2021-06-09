const { Router } = require('express');
const axios = require('axios');

const routes = Router();

async function type_1(type_data, filter = null) {
  try {
    let url = process.env.URL_API;
    if (filter !== null) {
      url += `?data=${filter}`;
    }
    const response_inmet = await axios.get(url);


    const [data_api] = response_inmet.data.filter(element => element.TIPO_DADO === type_data);

    if (data_api === undefined) {
      return { 'status': 400, 'mensage': "Este tipo de dados não existe" };
    }
    if (data_api.HORARIO === undefined) {
      return { 'status': 400, 'mensage': "Estamos processando os dados, porfavor tente mais tarde!" };
    }

    return {
      'status': 200,
      'mensage': data_api.VALUE_CAPTURED
    };
  } catch (error) {
    return {
      status: 400,
      mensage: error.response.data.mensage
    };
  }
}

async function type_2(type_data, filter = null) {
  try {
    let url = process.env.URL_API;
    if (filter !== null) {
      url += `?data=${filter}`;
    }
    const response_inmet = await axios.get(url);

    const [data_api] = response_inmet.data.filter(element => element.TIPO_DADO === type_data);

    if (data_api === undefined) {
      return { 'status': 400, 'mensage': "Este tipo de dados não existe" };
    }
    if (data_api.HORARIO === undefined) {
      return { 'status': 400, 'mensage': "Estamos processando os dados, porfavor tente mais tarde!" };
    }

    return {
      "status": 200,
      'CODIGO_ESTACAO': data_api.CODIGO_ESTACAO,
      'NOME_ESTACAO': data_api.NOME_ESTACAO,
      'LATITUDE': data_api.LATITUDE,
      'LONGITUDE': data_api.LONGITUDE,
      'HORARIO_COLETA': `${("0" + data_api.HORARIO).slice(-2)}:00`,
      'VALOR_OBSERVADO': data_api.VALUE_CAPTURED
    };
  }
  catch (error) {

    return {
      status: 400,
      mensage: error.response.data.mensage
    };
  }
}






routes.get("/precipitacao", async (request, response) => {
  const data_api = await type_1("CHUVAS");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

routes.get("/precipitacao/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_1("CHUVAS", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

routes.get("/tempmax", async (request, response) => {
  const data_api = await type_2("TEMP_MAX");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/tempmax/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_2("TEMP_MAX", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/tempmin", async (request, response) => {

  const data_api = await type_2("TEMP_MIN");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/tempmin/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_2("TEMP_MIN", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/tempmed", async (request, response) => {
  const data_api = await type_1("TEMP_AVG");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

routes.get("/tempmed/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_1("TEMP_AVG", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

routes.get("/umdmax", async (request, response) => {
  const data_api = await type_2("UMID_MAX");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/umdmax/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_2("UMID_MAX", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/umdmin", async (request, response) => {
  const data_api = await type_2("UMID_MIN");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/umdmin/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_2("UMID_MIN", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  delete data_api['status'];

  return response.status(200).json(data_api);
});

routes.get("/umdmed", async (request, response) => {
  const data_api = await type_1("UMID_AVG");

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

routes.get("/umdmed/:data", async (request, response) => {
  const { data } = request.params;
  const data_api = await type_1("UMID_AVG", filter = data);

  if (data_api.status === 400) {
    return response.status(400).json({ 'mensage': data_api.mensage });
  }

  return response.status(200).json({
    'VALOR_OBSERVADO': data_api.mensage
  });
});

module.exports = routes;