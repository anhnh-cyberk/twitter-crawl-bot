

async function main() {
  const delay = 6;
  while (true) {
    try {
      console.log("waiting for data from coinseeker");
      const userList = await loadTrackingFromCoinseeker();
      console.log("receive data from coinseeker");
      await insertNewUserToLake(userList);
      await updateProcessedAt(userList);
      console.log(`Process completed, retry in next ${delay}s`);
      await new Promise((resolve) => setTimeout(resolve, delay * 3600 * 1000));
    } catch (error) {
      console.error("An error occurred:", error);
    }
  }
}

main();
