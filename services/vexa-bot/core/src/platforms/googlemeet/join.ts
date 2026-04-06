import { Locator, Page } from "playwright";
import { log, randomDelay, callJoiningCallback } from "../../utils";
import { BotConfig } from "../../types";
import {
  googleNameInputSelectors,
  googleJoinButtonSelectors,
  googleMicrophoneButtonSelectors,
  googleCameraButtonSelectors
} from "./selectors";

function orChain(page: Page, selectors: string[]): Locator {
  let loc = page.locator(selectors[0]);
  for (let i = 1; i < selectors.length; i++) {
    loc = loc.or(page.locator(selectors[i]));
  }
  return loc;
}

async function muteMicAndCamera(page: Page): Promise<void> {
  try {
    await page.waitForTimeout(randomDelay(500));
    const micSelector = googleMicrophoneButtonSelectors[0];
    await page.click(micSelector, { timeout: 200 });
    await page.waitForTimeout(200);
  } catch {
    log("Microphone already muted or not found.");
  }

  try {
    await page.waitForTimeout(randomDelay(500));
    const cameraSelector = googleCameraButtonSelectors[0];
    await page.click(cameraSelector, { timeout: 200 });
    await page.waitForTimeout(200);
  } catch {
    log("Camera already off or not found.");
  }
}

/** Poll for guest name field vs signed-in pre-join (join controls without guest name). */
async function waitForPreJoinMode(
  page: Page,
  nameLoc: Locator,
  joinLoc: Locator,
  timeoutMs: number
): Promise<"guest" | "signed_in"> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await page.waitForTimeout(400);
    const nameVisible = await nameLoc.isVisible().catch(() => false);
    if (nameVisible) {
      log("Pre-join UI: guest (name field visible)");
      return "guest";
    }
    const joinVisible = await joinLoc.isVisible().catch(() => false);
    if (joinVisible) {
      await page.waitForTimeout(2500);
      if (await nameLoc.isVisible().catch(() => false)) {
        log("Pre-join UI: guest (name field appeared after join controls)");
        return "guest";
      }
      log("Pre-join UI: signed-in (join without guest name field)");
      return "signed_in";
    }
  }
  log("Pre-join UI: polling timed out; waiting for guest name field (last resort)...");
  await nameLoc.waitFor({ state: "visible", timeout: 120000 });
  return "guest";
}

export async function joinGoogleMeeting(
  page: Page,
  meetingUrl: string,
  botName: string,
  botConfig: BotConfig
): Promise<void> {
  await page.goto(meetingUrl, { waitUntil: "networkidle" });
  await page.bringToFront();

  await page.screenshot({ path: "/app/storage/screenshots/bot-checkpoint-0-after-navigation.png", fullPage: true });
  log("📸 Screenshot taken: After navigation to meeting URL");

  try {
    await callJoiningCallback(botConfig);
    log("Joining callback sent successfully");
  } catch (callbackError: any) {
    log(`Warning: Failed to send joining callback: ${callbackError.message}. Continuing with join process...`);
  }

  log("Waiting for page elements to settle after navigation...");
  await page.waitForTimeout(5000);

  await page.waitForTimeout(randomDelay(1000));
  log("Detecting guest vs signed-in Google Meet pre-join UI...");

  const nameLoc = orChain(page, googleNameInputSelectors);
  const joinLoc = orChain(page, googleJoinButtonSelectors);

  const mode = await waitForPreJoinMode(page, nameLoc, joinLoc, 120000);

  if (mode === "guest") {
    await page.screenshot({ path: "/app/storage/screenshots/bot-checkpoint-0-name-field-found.png", fullPage: true });
    log("📸 Screenshot taken: Name input field found");

    await page.waitForTimeout(randomDelay(1000));
    await nameLoc.fill(botName);
  } else {
    log("Skipping guest name fill (signed-in session).");
  }

  await muteMicAndCamera(page);

  await joinLoc.waitFor({ state: "visible", timeout: 60000 });
  await joinLoc.click();
  log(`${botName} joined the Google Meet Meeting.`);

  await page.screenshot({ path: "/app/storage/screenshots/bot-checkpoint-0-after-ask-to-join.png", fullPage: true });
  log("📸 Screenshot taken: After clicking 'Ask to join'");
}
