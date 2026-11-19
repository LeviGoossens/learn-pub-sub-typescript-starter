import amqp, {type ConfirmChannel} from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit
} from "../internal/gamelogic/gamelogic.js";
import {SimpleQueueType, subscribeJSON} from "../internal/pubsub/consume.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix
} from "../internal/routing/routing.js";
import {GameState} from "../internal/gamelogic/gamestate.js";
import {commandSpawn} from "../internal/gamelogic/spawn.js";
import {commandMove} from "../internal/gamelogic/move.js";
import {handlerMove, handlerPause, handlerWar} from "./handlers.js";
import {publishJSON, publishMsgPack} from "../internal/pubsub/publish.js";
import {type GameLog} from "../internal/gamelogic/logs.js"

export function publishGameLog(channel: ConfirmChannel, username: string, message: string) {
  let gameLog: GameLog = {currentTime: new Date(), message, username};
  return publishMsgPack(channel, ExchangePerilTopic, GameLogSlug + "." + username, gameLog);
}

async function main() {
  console.log("Starting Peril client...");
  const connString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connString);
  console.log("Connnection was succesful.");

  const confirmChannel = await conn.createConfirmChannel();

  let username = await clientWelcome();
  let myGameState = new GameState(username);

  await subscribeJSON(conn, ExchangePerilDirect, PauseKey + "." + username, PauseKey, SimpleQueueType.Transient, handlerPause(myGameState));
  await subscribeJSON(conn, ExchangePerilTopic, ArmyMovesPrefix + "." + username, ArmyMovesPrefix + ".*", SimpleQueueType.Transient, handlerMove(myGameState, confirmChannel));

  await subscribeJSON(conn, ExchangePerilTopic, WarRecognitionsPrefix, WarRecognitionsPrefix + ".*", SimpleQueueType.Durable, handlerWar(myGameState, confirmChannel));


  while (true) {
    const input = await getInput();
    const first_word = input[0];

    if (first_word === "spawn") {
      console.log("Sending a spawn message");
      try {
        commandSpawn(myGameState, input);
      } catch (error) {
        console.error(error);
      }
    } else if (first_word === "move") {
      console.log("Sending a move message");
      try {
        const am = commandMove(myGameState, input);
        await publishJSON(confirmChannel, ExchangePerilTopic, ArmyMovesPrefix + "." + username, am);
      } catch (error) {
        console.error(error);
      }
    } else if (first_word === "status") {
      commandStatus(myGameState);
    } else if (first_word === "help") {
      printClientHelp();
    } else if (first_word === "spam") {
      console.log("Spamming not allowed yet!");
      if (input[1] !== '' || input[1] !== undefined) {
        const number = parseInt(<string>input[1]);
        for (let i = 0; i < number; i++) {
          await publishMsgPack(confirmChannel, ExchangePerilTopic, `${GameLogSlug}.${username}`, {currentTime: new Date(), message: getMaliciousLog(), username});
        }
      }
    } else if (first_word === "quit") {
      console.log("We are quiting. Breaking out of loop.");
      printQuit();
      break;
    } else {
      console.error("command not understood. Please try again");
    }
  }



  process.on('SIGINT', async () => {
    await conn.close();
    console.log('Connection closed.');
    console.log('Program is shuting down.');
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
