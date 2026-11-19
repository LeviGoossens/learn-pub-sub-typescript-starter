import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import {declareAndBind, SimpleQueueType, subscribeMsgPack} from "../internal/pubsub/consume.js";
import {handlerLogs} from "../client/handlers.js";
function t() {

}
async function main() {
  console.log("Starting Peril server...");
  const connString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connString);
  console.log("Connnection was succesful.");

  const channel = await conn.createConfirmChannel();

  await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, GameLogSlug + ".*", SimpleQueueType.Durable, handlerLogs());

  printServerHelp();
  while (true) {
    const input = await getInput();
    const first_word = input[0];

    let playingState: PlayingState;

    if (first_word === "pause") {
      console.log("Sending a pause message");
      playingState = {isPaused: true};
      publishJSON(channel, ExchangePerilDirect, PauseKey, playingState);
    }
    else if (first_word === "resume") {
      console.log("Sending a resume message");
      playingState = {isPaused: false};
      publishJSON(channel, ExchangePerilDirect, PauseKey, playingState);
    }
    else if (first_word === "quit") {
      console.log("We are quiting. Breaking out of loop.");
      break;
    }
    else {
      console.log("command not understood. Please try again");
    }
  };


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
