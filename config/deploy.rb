# config valid only for current version of Capistrano
lock "3.4.0"

set :application, "adv_se_worker"
set :repo_url, "https://github.com/dschoeni/#{fetch(:application)}.git"

# Default branch is :master
# ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp
set :branch, "master"

# SSH connection
# --------------
set :ssh_options, {
    keys: %w(~/.ssh/aws_ase15_1),
    # forward_agent: true,
    auth_methods: %w(publickey)
}

# Remote machine
# --------------
set :deploy_user, "deploy"
set :apps_user, "apps"

# Default deploy_to directory is /var/www/my_app_name
set :deploy_to, "/var/apps/#{fetch(:application)}"

# Default value for :scm is :git
# set :scm, :git

# Default value for :format is :pretty
# set :format, :pretty

# Default value for :log_level is :debug
# set :log_level, :debug

# Default value for :pty is false
# set :pty, true

# Default value for :linked_files is []
# set :linked_files, fetch(:linked_files, []).push("config/database.yml", "config/secrets.yml")

# Default value for linked_dirs is []
set :linked_dirs, fetch(:linked_dirs, []).push("dist", "node_modules")

# Default value for default_env is {}
# set :default_env, { path: "/opt/ruby/bin:$PATH" }

# Default value for keep_releases is 5
# set :keep_releases, 5

namespace :deploy do
  task :build do
    on roles(:app), in: :groups, limit: 3, wait: 10 do
      within release_path do
        execute *%w[ sudo npm install ]
      end
    end
  end

  task :restart do
    on roles(:app), in: :groups, limit: 3, wait: 10 do
      execute "sudo sv restart #{fetch(:application)}"
    end
  end

  after "deploy:updated", "deploy:build"
  # As of Capistrano 3.1, the `deploy:restart` task is not called automatically.
  after "deploy:publishing", "deploy:restart"

end
