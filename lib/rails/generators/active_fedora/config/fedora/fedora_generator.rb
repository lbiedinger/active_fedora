require 'rails/generators'

module ActiveFedora
  class Config::FedoraGenerator < Rails::Generators::Base
    source_root File.expand_path('../templates', __FILE__)

    def copy_yaml
      copy_file('fedora.yml', 'config/fedora.yml')
    end

    def copy_conf_directory
      directory('fedora_conf', 'fedora_conf')
    end

  end
end
