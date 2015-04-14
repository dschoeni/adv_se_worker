var pkg = require('./package.json');
var appName = 'adv_se_worker'

module.exports = function (shipit) {
  require('shipit-deploy')(shipit);
  require('shipit-npm')(shipit);
  require('shipit-shared')(shipit);

  shipit.initConfig({
    default: {
      workspace: '$HOME/git/' + appName,
      deployTo: '/var/apps/' + appName,
      repositoryUrl: pkg.repository.url,
      ignores: ['.git', 'node_modules'],
      keepReleases: 3,
      key: '$HOME/.ssh/aws_ase15_1',
      shared: {
        dirs: [
          'dist',
          'node_modules',
        ],
    },

    production: {
      servers: 'AWS_IP'
    }
  });

  shipit.task('restart-server', function () {
      return shipit.remote('sudo sv restart ' + appName);
  });

  shipit.on('published', function () {
      shipit.start('restart-server');
  });

  shipit.task('pwd', function () {
    return shipit.remote('pwd');
  });
};
