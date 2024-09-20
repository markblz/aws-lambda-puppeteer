// Import necessary AWS SDK clients
const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses'); // Import SES client
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns'); // Import SNS client (para SMS, se necessário)

// Initialize AWS SDK clients
const dynamoClient = new DynamoDBClient({ region: 'sa-east-1' });
const sesClient = new SESClient({ region: 'sa-east-1' }); // Initialize SES client
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
  let decisionMatch = false;
  if (userDecisionTypes.length > 0 && publicationData.tipoDecisao) {
    const tipoDecisaoNormalizado = removeSpecialCharacters(publicationData.tipoDecisao.toLowerCase());
    for (const decisionType of userDecisionTypes) {
      if (tipoDecisaoNormalizado.includes(decisionType)) {
        console.log(`Decision type match for user ${userId}: ${decisionType}`);
        matchedFields.push(`Decision Type: "${decisionType.toUpperCase()}"`);
        decisionMatch = true;
        break;
      }
    }
  } else {
    // Se o usuário não especificou tipos de decisão, considerar como match
    decisionMatch = true;
  }

  const matchFound = matchedFields.length > 0 && decisionMatch;

  return { matchFound, matchedFields };
}

// Função para enviar notificação para o usuário
async function sendNotification(user, publicationData, matchedFields) {
  const contactMethod = user.contactMethod.S;
  const phoneNumber = user.phoneNumber?.S || null;
  const emailAddress = user.emailAddress?.S || null; // Novo campo para email
  const userName = user.userName ? user.userName.S : 'Usuário'; // Recuperar o nome do usuário, se disponível

  const detectionTime = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }); // Ajuste o fuso horário conforme necessário

  const message = `Olá ${userName}! 😄

Acabei de dar uma olhada rápida às ${detectionTime} e, adivinha? Encontrei uma publicação que você estava esperando:

📌 **Número da Publicação:** ${publicationData.numeroPublicacao}
📅 **Data:** ${publicationData.dataPublicacao}
⚖️ **Tipo de Decisão:** ${publicationData.tipoDecisao}
📝 **Conteúdo:** ${publicationData.textoPublicacao}

🔍 **O que chamou sua atenção:**
${matchedFields.map(field => `- ${field}`).join('\n')}

Parece que alguém estava realmente querendo que você soubesse disso! 😉 Não se preocupe, estou sempre de olho para você.

Abraços virtuais,
Seu Amigo de Notificações 📡
`;

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
    if (!emailAddress) {
      console.error(`Nenhum endereço de email fornecido para o usuário ${user.userId.S}`);
      return;
    }

    // Enviar Email via SES
    const emailParams = {
      Destination: {
        ToAddresses: [emailAddress],
      },
      Message: {
        Body: {
          Html: {
            Charset: "UTF-8",
            Data: `<p>Olá ${userName}! 😄</p>

<p>Acabei de dar uma olhada rápida às ${detectionTime} e, adivinha? Encontrei uma publicação que você estava esperando:</p>

<p><strong>Número da Publicação:</strong> ${publicationData.numeroPublicacao}<br>
<strong>Data:</strong> ${publicationData.dataPublicacao}<br>
<strong>Tipo de Decisão:</strong> ${publicationData.tipoDecisao}<br>
<strong>Conteúdo:</strong> ${publicationData.textoPublicacao}</p>

<p><strong>O que chamou sua atenção:</strong><br>
${matchedFields.map(field => `- ${field}`).join('<br>')}</p>

<p>Parece que alguém estava realmente querendo que você soubesse disso! 😉 Não se preocupe, estou sempre de olho para você.</p>

<p>Abraços virtuais,<br>
Seu Amigo de Notificações 📡</p>`,
          },
          Text: {
            Charset: "UTF-8",
            Data: message.replace(/\n/g, "\n"),
          },
        },
        Subject: {
          Charset: 'UTF-8',
          Data: `Nova Publicação: ${publicationData.numeroPublicacao}`,
        },
      },
      Source: 'seu-email@dominio.com', // Substitua pelo seu email verificado no SES
    };

    try {
      await sesClient.send(new SendEmailCommand(emailParams));
      console.log(`Email enviado para ${emailAddress}`);
    } catch (err) {
      console.error(`Erro ao enviar Email para ${emailAddress}:`, err);
    }
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
