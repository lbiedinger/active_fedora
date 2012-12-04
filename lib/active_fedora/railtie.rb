module ActiveFedora
  class Railtie < Rails::Railtie
    rake_tasks do
      load "tasks/active_fedora.rake"
    end
    generators do
      require(
        'rails/generators/active_fedora/config/config_generator'
      )
    end
  end
end
