// Import necessary AWS SDK clients
const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns'); // Import SNS client

// Initialize AWS SDK clients
const dynamoClient = new DynamoDBClient({ region: 'sa-east-1' });
const snsClient = new SNSClient({ region: 'sa-east-1' }); // Initialize SNS client

exports.handler = async (event) => {
  try {
    console.log('Received event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
      if (record.eventName === 'INSERT') {
        const newItem = record.dynamodb.NewImage;

        // Extrair campos relevantes do novo item
        const publicationData = {
          numeroPublicacao: newItem.numeroPublicacao.N,
          textoPublicacao: newItem.textoPublicacao.S,
          decisao: JSON.parse(newItem.decisao.S),
          fontePublicacao: JSON.parse(newItem.fontePublicacao.S),
          dataPublicacao: newItem.dataPublicacao.S,
          id: newItem.id.N,
          // Novos campos
          tribunalSigla: newItem.tribunalSigla?.S || null,
          tipoDecisao: newItem.tipoDecisao?.S || null,
          partes: newItem.partes?.L.map(parte => ({
            nomeParte: parte.M.nomeParte?.S || null,
            advogados: parte.M.advogados?.L.map(adv => ({
              nomeAdvogado: adv.M.nomeAdvogado?.S || null,
              numeroOAB: adv.M.numeroOAB?.S || null,
              ufSigla: adv.M.ufSigla?.S || null,
            })) || []
          })) || []
        };

        console.log('Processing new publication:', publicationData.numeroPublicacao);

        // Extraindo nomes das partes e advogados para busca
        const partesNomes = publicationData.partes.map(parte => parte.nomeParte).filter(nome => nome);
        const advogadosNomes = publicationData.partes.flatMap(parte => parte.advogados.map(adv => adv.nomeAdvogado)).filter(nome => nome);
        const todosNomes = [...partesNomes, ...advogadosNomes];

        const keywords = extractKeywords(publicationData);

        // Recuperar todas as preferências dos usuários
        const users = await getAllUserPreferences();

        // Comparar publicações com preferências dos usuários
        for (const user of users) {
          const { matchFound, matchedFields } = checkForMatches(user, publicationData, todosNomes, keywords);

          if (matchFound) {
            // Enviar notificação para o usuário
            await sendNotification(user, publicationData, matchedFields);
          }
        }
      }
    }
  } catch (error) {
    console.error('Error processing DynamoDB stream event:', error);
  }
};

// Função para extrair palavras-chave dos dados da publicação
function extractKeywords(publicationData) {
  const text = publicationData.textoPublicacao.toLowerCase();
  const words = text.match(/\b(\w+)\b/g); // Extração simples de palavras
  return words || [];
}

// Função para recuperar todas as preferências dos usuários
async function getAllUserPreferences() {
  const params = {
    TableName: 'UserPreferences',
  };

  const users = [];
  let lastEvaluatedKey = null;

  do {
    const result = await dynamoClient.send(new ScanCommand(params));
    users.push(...result.Items);
    lastEvaluatedKey = result.LastEvaluatedKey;
    params.ExclusiveStartKey = lastEvaluatedKey;
  } while (lastEvaluatedKey);

  return users;
}

// Função para verificar correspondências entre dados da publicação e preferências do usuário
function checkForMatches(user, publicationData, todosNomes, publicationKeywords) {
  const userId = user.userId.S;
  const userName = user.userName ? user.userName.S : 'Usuário';

  // Recuperar preferências do usuário
  const userKeywords = user.keywords ? user.keywords.L.map(k => k.S.toLowerCase()) : [];
  const userClientNames = user.clientNames ? user.clientNames.L.map(c => c.S.toLowerCase()) : [];
  const userLawyerNames = user.lawyerNames ? user.lawyerNames.L.map(l => l.S.toLowerCase()) : [];
  const userDecisionTypes = user.decisionTypes ? user.decisionTypes.L.map(d => d.S.toLowerCase()) : [];

  // Normalizar nomes das partes e advogados
  const normalizedNomes = todosNomes.map(nome => removeSpecialCharacters(nome.toLowerCase()));

  // Inicializar array para armazenar quais campos foram correspondidos
  const matchedFields = [];

  // Verificar correspondências nas palavras-chave
  for (const keyword of userKeywords) {
    if (publicationKeywords.includes(keyword)) {
      console.log(`Keyword match for user ${userId}: ${keyword}`);
      matchedFields.push(`Keyword: "${keyword}"`);
      break; // Evita múltiplas correspondências da mesma categoria
    }
  }

  // Verificar correspondências nos nomes de clientes
  for (const clientName of userClientNames) {
    if (normalizedNomes.some(nome => nome.includes(clientName))) {
      console.log(`Client name match for user ${userId}: ${clientName}`);
      matchedFields.push(`Client Name: "${clientName}"`);
      break;
    }
  }

  // Verificar correspondências nos nomes de advogados
  for (const lawyerName of userLawyerNames) {
    if (normalizedNomes.some(nome => nome.includes(lawyerName))) {
      console.log(`Lawyer name match for user ${userId}: ${lawyerName}`);
      matchedFields.push(`Lawyer Name: "${lawyerName}"`);
      break;
    }
  }

  // Verificar correspondências no tipo de decisão
  if (userDecisionTypes.length > 0 && publicationData.tipoDecisao) {
    const tipoDecisaoNormalizado = removeSpecialCharacters(publicationData.tipoDecisao.toLowerCase());
    for (const decisionType of userDecisionTypes) {
      if (tipoDecisaoNormalizado.includes(decisionType)) {
        console.log(`Decision type match for user ${userId}: ${decisionType}`);
        matchedFields.push(`Decision Type: "${decisionType.toUpperCase()}"`);
        break;
      }
    }
  }

  const matchFound = matchedFields.length > 0;

  return { matchFound, matchedFields };
}

// Função para enviar notificação para o usuário
async function sendNotification(user, publicationData, matchedFields) {
  const contactMethod = user.contactMethod.S;
  const phoneNumber = user.phoneNumber?.S || null;
  const userName = user.userName ? user.userName.S : 'Usuário'; // Recuperar o nome do usuário, se disponível

  const detectionTime = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }); // Ajuste o fuso horário conforme necessário

  const message = `Olá ${userName}!\n\n` +
                  `Nova publicação detectada às ${detectionTime} que corresponde às suas preferências:\n\n` +
                  `Número da Publicação: ${publicationData.numeroPublicacao}\n` +
                  `Data: ${publicationData.dataPublicacao}\n` +
                  `Tipo de Decisão: ${publicationData.tipoDecisao}\n` +
                  `Conteúdo: ${publicationData.textoPublicacao}\n\n` +
                  `Preferências Correspondidas:\n` +
                  `${matchedFields.map(field => `- ${field}`).join('\n')}\n\n` +
                  `Atenciosamente,\nSeu robô de notificações`;

  if (contactMethod === 'sms') {
    if (!phoneNumber) {
      console.error(`Nenhum número de telefone fornecido para o usuário ${user.userId.S}`);
      return;
    }

    // Enviar SMS via SNS
    const params = {
      PhoneNumber: phoneNumber,
      Message: message,
    };

    try {
      await snsClient.send(new PublishCommand(params));
      console.log(`SMS enviado para ${phoneNumber}`);
    } catch (err) {
      console.error(`Erro ao enviar SMS para ${phoneNumber}:`, err);
    }
  } else if (contactMethod === 'email') {
    // Lógica de envio de email (se desejar manter notificações por email)
    // Para simplicidade, você pode remover este bloco else-if se não estiver usando email
    console.log(`Notificações por email não estão configuradas.`);
  } else {
    console.log(`Método de contato não suportado para o usuário ${user.userId.S}: ${contactMethod}`);
  }
}

// Função para remover caracteres especiais e acentos
function removeSpecialCharacters(text) {
  if (typeof text !== 'string') {
    return text;
  }

  const nfkdForm = text.normalize('NFD');
  const withoutAccent = nfkdForm.replace(/[\u0300-\u036f]/g, '');

  return withoutAccent;
}
