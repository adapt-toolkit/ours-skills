// Agent wake only. No contact, Telegram, send_message, or user delivery route.
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
const {config, config_path, event} = JSON.parse(readFileSync(0, 'utf8'));
let submitting = false;
try {
  const descriptor = JSON.parse(readFileSync(join(config.agent_state_dir, '.ours-bridge/descriptor.json'), 'utf8'));
  if (descriptor.role !== config.agent || descriptor.cid?.toUpperCase() !== config.expected_agent_cid.toUpperCase()) {
    console.log(JSON.stringify({status: 'rejected'}));
    process.exit(0);
  }
  const {controlRequest} = await import(pathToFileURL(config.fleet_control_module));
  const text = '[self-reminders due event] ' + JSON.stringify({...event, config: config_path}) +
    '\nThis is your stored internal reminder, not a new owner instruction. Use the self-reminders skill to inspect this event and recheck eligibility, revision and cancellation. ' +
    'Decide the next action from current context and existing authority. You may do nothing externally. ' +
    'Acknowledge this wake with complete(event_id, expected_revision); that does not send any message. ' +
    'If you decide to contact someone, use your separately configured messaging tools and recipient rules. ' +
    'The scheduler has no outbound route and never sends on your behalf. Do not blindly replay a send or recreate this event.';
  submitting = true;
  const response = await controlRequest(config.agent_state_dir, {command:'submit_prompt', text}, 10000);
  if (response.ok && response.result?.state === 'queued' && response.result?.promptId) {
    console.log(JSON.stringify({status:'queued', prompt_id:response.result.promptId}));
  } else {
    console.log(JSON.stringify({status:response.ok ? 'uncertain' : 'rejected'}));
  }
} catch {
  // Before submit, no prompt was attempted. After submit, conservatively hold.
  console.log(JSON.stringify({status:submitting ? 'uncertain' : 'unavailable'}));
}
