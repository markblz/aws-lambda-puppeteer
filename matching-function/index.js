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
  const contactDetails = user.contactDetails ? user.contactDetails.S : null;

  const message = `New publication matching your preferences:\n\n` +
                  `Publication Number: ${publicationData.numeroPublicacao}\n` +
                  `Date: ${publicationData.dataPublicacao}\n` +
                  `Content: ${publicationData.textoPublicacao}\n\n` +
                  `Regards,\nYour Notification System`;

  if (contactMethod === 'sms') {
    const phoneNumber = user.phoneNumber.S; // Assume phone number is stored in 'phoneNumber' attribute
    if (!phoneNumber) {
      console.error(`No phone number provided for user ${user.userId.S}`);
      return;
    }

    // Send SMS via SNS
    const params = {
      PhoneNumber: phoneNumber,
      Message: message,
    };

    try {
      await snsClient.send(new PublishCommand(params));
      console.log(`SMS sent to ${phoneNumber}`);
    } catch (err) {
      console.error(`Error sending SMS to ${phoneNumber}:`, err);
    }
  } else if (contactMethod === 'email') {
    // Email sending logic (if you want to keep email notifications)
    // For simplicity, you can remove this else-if block if you're not using email anymore
    console.log(`Email notifications are not configured.`);
  } else {
    console.log(`Unsupported contact method for user ${user.userId.S}: ${contactMethod}`);
  }
}
