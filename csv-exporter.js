// ------------------------------------
// Logging
var log4js = require('log4js'),
    log = log4js.getLogger('csv-exporter');
log4js.configure({
  appenders: [
    { type: 'console'}
  ]});

// ------------------------------------
// Config
var yaml = require('js-yaml'),
    fs = require('fs'),
    cfg = yaml.safeLoad(fs.readFileSync('settings.yml', 'utf8'));

// ------------------------------------
var request = require('request'),
    util = require('util');

var options = {
  url: 'https://api.opensensors.io/v1/messages/topic/' + cfg.osio.topic,
  headers: { Authorization: 'api-key ' + cfg.osio.api_key }
};

if (fs.existsSync(cfg.output)) {
  log.error('output file exists: ' + cfg.output);
  process.exit(1);
}

fs.writeFileSync(cfg.output, "device,topic,date,payload\r\n", {encoding: 'utf8', flag: 'a'});

function callback(error, response, body) {
  var bod = JSON.parse(body);
  if (error) {
    log.error(error);
  } else if (response.statusCode != 200) {
    log.error('status ' + response.statusCode);
    log.error(bod);
  } else {
    for (var i = 0; i < bod.messages.length; i++) {
      var m = bod.messages[i];
      fs.writeFileSync(cfg.output, util.format('%s,%s,%s,%s\r\n', m.device, m.topic, m.date, m.payload.text), {encoding: 'utf8', flag: 'a'});
    }
    if (bod.next) {
      log.info('Fetching next batch of messages on topic: ' + cfg.osio.topic);
      var opts = options;
      opts.url = 'https://api.opensensors.io' + bod.next;
      request(opts, callback);
    } else {
      log.info('Done! Output written to ' + cfg.output);
    }
  }
}

log.info('Fetching first batch of messages on topic: ' + cfg.osio.topic);

request(options, callback);
