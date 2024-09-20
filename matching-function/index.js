// Import necessary AWS SDK clients
const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');
const { SESClient, SendEmailCommand } = require('@aws-sdk/client-ses');

// Initialize AWS SDK clients
const dynamoClient = new DynamoDBClient({ region: 'sa-east-1' });
const sesClient = new SESClient({ region: 'sa-east-1' });

exports.handler = async (event) => {
  try {
    console.log('Received event:', JSON.stringify(event, null, 2));

    for (const record of event.Records) {
      if (record.eventName === 'INSERT') {
        const newItem = record.dynamodb.NewImage;

        // Extract relevant fields from the new item
        const publicationData = {
          numeroPublicacao: newItem.numeroPublicacao.N,
          textoPublicacao: newItem.textoPublicacao.S,
          decisao: JSON.parse(newItem.decisao.S),
          fontePublicacao: JSON.parse(newItem.fontePublicacao.S),
          dataPublicacao: newItem.dataPublicacao.S,
          id: newItem.id.N,
        };

        console.log('Processing new publication:', publicationData.numeroPublicacao);

        // Extract parties, clients, lawyers, etc.
        const parties = publicationData.decisao.partes.map(p => p.nome);
        const keywords = extractKeywords(publicationData);

        // Retrieve all user preferences
        const users = await getAllUserPreferences();

        // Match publications to user preferences
        for (const user of users) {
          const matches = checkForMatches(user, publicationData, parties, keywords);

          if (matches) {
            // Send notification to the user
            await sendNotification(user, publicationData);
          }
        }
      }
    }
  } catch (error) {
    console.error('Error processing DynamoDB stream event:', error);
  }
};

// Function to extract keywords from publication data
function extractKeywords(publicationData) {
  // Implement logic to extract keywords from the publication text or other fields
  // For simplicity, let's return an array of words from 'textoPublicacao'
  const text = publicationData.textoPublicacao.toLowerCase();
  const words = text.match(/\b(\w+)\b/g); // Simple word extraction
  return words || [];
}

// Function to retrieve all user preferences
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

// Function to check for matches between publication data and user preferences
function checkForMatches(user, publicationData, parties, publicationKeywords) {
  const userId = user.userId.S;

  // Retrieve user preferences
  const userKeywords = user.keywords ? user.keywords.L.map(k => k.S.toLowerCase()) : [];
  const userClientNames = user.clientNames ? user.clientNames.L.map(c => c.S.toLowerCase()) : [];
  const userLawyerNames = user.lawyerNames ? user.lawyerNames.L.map(l => l.S.toLowerCase()) : [];

  // Check for matches
  let matchFound = false;

  // Check keywords
  for (const keyword of userKeywords) {
    if (publicationKeywords.includes(keyword)) {
      console.log(`Keyword match for user ${userId}: ${keyword}`);
      matchFound = true;
      break;
    }
  }

  // Check client names
  if (!matchFound) {
    for (const clientName of userClientNames) {
      if (parties.some(p => p.toLowerCase().includes(clientName))) {
        console.log(`Client name match for user ${userId}: ${clientName}`);
        matchFound = true;
        break;
      }
    }
  }

  // Check lawyer names
  if (!matchFound) {
    for (const lawyerName of userLawyerNames) {
      if (parties.some(p => p.toLowerCase().includes(lawyerName))) {
        console.log(`Lawyer name match for user ${userId}: ${lawyerName}`);
        matchFound = true;
        break;
      }
    }
  }

  return matchFound;
}

// Function to send notification to the user
async function sendNotification(user, publicationData) {
  const contactMethod = user.contactMethod.S;
  const contactDetails = user.contactDetails.S;

  if (contactMethod === 'email') {
    const subject = 'New Publication Alert';
    const body = `A new publication matching your preferences has been found:\n\n` +
                 `Publication Number: ${publicationData.numeroPublicacao}\n` +
                 `Date: ${publicationData.dataPublicacao}\n` +
                 `Content: ${publicationData.textoPublicacao}\n\n` +
                 `Regards,\nYour Notification System`;

    const emailParams = {
      Destination: {
        ToAddresses: [contactDetails],
      },
      Message: {
        Body: {
          Text: {
            Data: body,
          },
        },
        Subject: {
          Data: subject,
        },
      },
      Source: 'your-verified-email@example.com', // Replace with your verified email
    };

    try {
      await sesClient.send(new SendEmailCommand(emailParams));
      console.log(`Email sent to ${contactDetails}`);
    } catch (err) {
      console.error(`Error sending email to ${contactDetails}:`, err);
    }
  } else {
    console.log(`Unsupported contact method for user ${user.userId.S}: ${contactMethod}`);
  }
}