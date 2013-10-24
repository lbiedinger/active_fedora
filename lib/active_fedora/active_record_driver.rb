require 'active_record'
module ActiveFedora 

  # Provide low-level access to the Fedora Commons REST API
  module ActiveRecordDriver
    
    include Rubydora::FedoraUrlHelpers
    extend ActiveSupport::Concern
    include ActiveSupport::Benchmarkable    
    extend Deprecation

    class Datastream < ActiveRecord::Base
    end
    class RepoObject < ActiveRecord::Base
    end
    class Sequence < ActiveRecord::Base
    end



    VALID_CLIENT_OPTIONS = [:user, :password, :timeout, :open_timeout, :ssl_client_cert, :ssl_client_key]

    included do
      include Hooks
      [:ingest, :modify_object, :purge_object, :set_datastream_options, :add_datastream, :modify_datastream, :purge_datastream, :add_relationship, :purge_relationship].each do |h|
        define_hook "before_#{h}".to_sym
      end

      define_hook :after_ingest
    end

    def describe options = {}
      query_options = options.dup
      query_options[:xml] ||= 'true'
      raise "not implemented"
      client[describe_repository_url(query_options)].get
    rescue Exception => exception
      rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @return [String]
    def next_pid options = {}
      namespace = options.fetch(:namespace, 'changeme')
      seq = Sequence.first_or_create
      seq.with_lock do
        seq.value += 1
        seq.save!
      end
      "<resp xmlns:fedora=\"http://www.fedora.info/definitions/1/0/management/\"><fedora:pid>#{namespace}:#{(seq.value)}</fedora:pid></resp>"
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @return [String]
    def find_objects options = {}, &block_response
      query_options = options.dup
      raise ArgumentError,"Cannot have both :terms and :query parameters" if query_options[:terms] and query_options[:query]
      query_options[:resultFormat] ||= 'xml'

      resource = client[find_objects_url(query_options)]
      if block_given?
        resource.query_options[:block_response] = block_response
      end 
      raise "not implemented"
      return resource.get
    rescue Exception => exception
      rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def object options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)

      obj = RepoObject.where(pid: pid).first
      raise RestClient::ResourceNotFound unless obj
      "<objectProfile>
      <objLabel>#{obj.label}</objLabel>
      <objState>#{obj.state}</objState>
      <objCreateDate>#{Date.today()}</objCreateDate><objLastModDate>#{Date.today()}</objLastModDate></objectProfile>"
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def ingest options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)

      if pid.nil?
        return mint_pid_and_ingest options
      end

      file = query_options.delete(:file)
      raise "I don't deal with fixtures yet." if file
      #assigned_pid = client[object_url(pid, query_options)].post((file.dup if file), :content_type => 'text/xml')
      RepoObject.create!(pid: pid, label: query_options[:label])
      assigned_pid = "info:fedora/#{pid}"
      run_hook :after_ingest, :pid => assigned_pid, :file => file, :options => options
      assigned_pid
    end

    def mint_pid_and_ingest options = {}
      query_options = options.dup
      file = query_options.delete(:file)

      raise "not implemented"
      assigned_pid = client[new_object_url(query_options)].post((file.dup if file), :content_type => 'text/xml')
      run_hook :after_ingest, :pid => assigned_pid, :file => file, :options => options
      assigned_pid
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def export options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      raise ArgumentError, "Must have a pid" unless pid
      raise "not implemented"
      client[export_object_url(pid, query_options)].get
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def modify_object options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      run_hook :before_modify_object, :pid => pid, :options => options
      RepoObject.where(pid: pid).first.update(options)
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def purge_object options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      run_hook :before_purge_object, :pid => pid, :options => options
      Datastream.where(pid: pid).destroy_all
      RepoObject.where(pid: pid).destroy_all
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def object_versions options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      query_options[:format] ||= 'xml'
      raise ArgumentError, "Must have a pid" unless pid
      raise "not implemented"
      client[object_versions_url(pid, query_options)].get
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def object_xml options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      raise ArgumentError, "Missing required parameter :pid" unless pid
      query_options[:format] ||= 'xml'
      raise "not implemented"
      client[object_xml_url(pid, query_options)].get
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @option options [String] :asOfDateTime
    # @option options [String] :validateChecksum
    # @return [String]
    def datastream options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      raise ArgumentError, "Missing required parameter :pid" unless pid

      if dsid.nil?
        #raise ArgumentError, "Missing required parameter :dsid" unless dsid
        Deprecation.warn(RestApiClient, "Calling Rubydora::RestApiClient#datastream without a :dsid is deprecated, use #datastreams instead")
        return datastreams(options)
      end
      query_options[:format] ||= 'xml'
      val = nil

      ds = Datastream.where(pid: pid, dsid: dsid).first
      raise RestClient::ResourceNotFound unless ds
      "<datastreamProfile>
          <dsSize>#{ds.content ? ds.content.size : 0}</dsSize>
          <dsMIME>#{ds.content_type}</dsMIME>
          <dsLabel>#{ds.label}</dsLabel>
          <dsVersionable>#{ds.versionable}</dsVersionable>
        </datastreamProfile>"
    end

    def datastreams options = {}
      unless options[:dsid].nil?
        #raise ArgumentError, "Missing required parameter :dsid" unless dsid
        Deprecation.warn(RestApiClient, "Calling Rubydora::RestApiClient#datastreams with a :dsid is deprecated, use #datastream instead")
        return datastream(options)
      end
      query_options = options.dup
      pid = query_options.delete(:pid)
      raise ArgumentError, "Missing required parameter :pid" unless pid
      query_options[:format] ||= 'xml'
      val = nil
      content = Datastream.where(pid: pid).map { |ds| "<datastream dsid='#{ds.dsid}'></datastream>" }
      "<objectDatastreams>#{content.join}</objectDatastreams>"
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def set_datastream_options options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      run_hook :before_set_datastream_options, :pid => pid, :dsid => dsid, :options => options
      client[datastream_url(pid, dsid, query_options)].put nil
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def datastream_versions options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      raise ArgumentError, "Must supply dsid" unless dsid
      query_options[:format] ||= 'xml'
      raise "not implemented"
      client[datastream_history_url(pid, dsid, query_options)].get
    rescue RestClient::ResourceNotFound => e
      #404 Resource Not Found: No datastream history could be found. There is no datastream history for the digital object "changeme:1" with datastream ID of "descMetadata
      return nil
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    alias_method :datastream_history, :datastream_versions

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def datastream_dissemination options = {}, &block_response
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      # method = query_options.delete(:method)
      # method ||= :get
      # raise self.class.name + "#datastream_dissemination requires a DSID" unless dsid
      # if block_given?
      #   resource = safe_subresource(datastream_content_url(pid, dsid, query_options), :block_response => block_response)
      # else
      #   resource = client[datastream_content_url(pid, dsid, query_options)]
      # end
      # val = nil
      # benchmark "Loaded datastream content #{pid}/#{dsid}", :level=>:debug do
      # raise "not implemented"
      #   val = resource.send(method)
      # end
      # val
      ds = Datastream.where(pid: pid, dsid: dsid).first
      raise "can't find datastream #{pid}, #{dsid}" unless ds
      ds.content
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def add_datastream options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      file = query_options.delete(:content)
      # In ruby 1.8.7 StringIO (file) responds_to? :path, but it always returns nil,  In ruby 1.9.3 StringIO doesn't have path.
      # When we discontinue ruby 1.8.7 support we can remove the `|| ''` part.
      content_type = query_options.delete(:content_type) || query_options[:mimeType] || (MIME::Types.type_for(file.path || '').first if file.respond_to? :path) || 'application/octet-stream'
      run_hook :before_add_datastream, :pid => pid, :dsid => dsid, :file => file, :options => options
      str = file.respond_to?(:read) ? file.read : file
      file.rewind if file.respond_to?(:rewind)
      Datastream.create(pid: pid, dsid: dsid, content: str, content_type: content_type.to_s, label: query_options[:dsLabel], versionable: query_options[:versionable])
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def modify_datastream options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      file = query_options.delete(:content)
      # In ruby 1.8.7 StringIO (file) responds_to? :path, but it always returns nil,  In ruby 1.9.3 StringIO doesn't have path.
      # When we discontinue ruby 1.8.7 support we can remove the `|| ''` part.
      content_type = query_options.delete(:content_type) || query_options.delete(:mimeType) || (MIME::Types.type_for(file.path || '').first if file.respond_to? :path) || 'application/octet-stream'

      rest_client_options = {}
      if file
        rest_client_options[:multipart] = true
        rest_client_options[:content_type] = content_type
      end

      run_hook :before_modify_datastream, :pid => pid, :dsid => dsid, :file => file, :content_type => content_type, :options => options
      str = file.respond_to?(:read) ? file.read : file
      file.rewind if file.respond_to?(:rewind)
      ds = if dsid == "RELS-EXT"
        Datastream.where(pid: pid, dsid: dsid).first_or_create
      else 
        Datastream.where(pid: pid, dsid: dsid).first
      end
      raise "can't find datastream #{pid}, #{dsid}" unless ds
      optional_args = {}
      optional_args[:label] = query_options[:dsLabel] if query_options[:dsLabel]
      ds.update({content_type: content_type, content: file}.merge(optional_args))
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :dsid
    # @return [String]
    def purge_datastream options = {}
      query_options = options.dup
      pid = query_options.delete(:pid)
      dsid = query_options.delete(:dsid)
      run_hook :before_purge_datastream, :pid => pid, :dsid => dsid
      raise "not implemented"
      client[datastream_url(pid, dsid, query_options)].delete
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def relationships options = {}
      query_options = options.dup
      pid = query_options.delete(:pid) || query_options[:subject]
      raise ArgumentError, "Missing required parameter :pid" unless pid
      query_options[:format] ||= 'xml'
      raise "not implemented"
      client[object_relationship_url(pid, query_options)].get
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def add_relationship options = {}
      query_options = options.dup
      pid = query_options.delete(:pid) || query_options[:subject]
      run_hook :before_add_relationship, :pid => pid, :options => options
      raise "not implemented"
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @return [String]
    def purge_relationship options = {}
      query_options = options.dup
      pid = query_options.delete(:pid) || query_options[:subject]
      run_hook :before_purge_relationship, :pid => pid, :options => options
      raise "not implemented"
      client[object_relationship_url(pid, query_options)].delete
    rescue Exception => exception
        rescue_with_handler(exception) || raise
    end

    # {include:RestApiClient::API_DOCUMENTATION}
    # @param [Hash] options
    # @option options [String] :pid
    # @option options [String] :sdef
    # @option options [String] :method
    # @return [String]
    def dissemination options = {}, &block_response
      query_options = options.dup
      pid = query_options.delete(:pid)
      sdef = query_options.delete(:sdef)
      method = query_options.delete(:method)
      query_options[:format] ||= 'xml' unless pid and sdef and method
      if block_given?
        resource = safe_subresource(dissemination_url(pid,sdef,method,query_options), :block_response => block_response)
      else
        resource = client[dissemination_url(pid,sdef,method,query_options)]
      end
      raise "not implemented"
      resource.get

    rescue Exception => exception
        rescue_with_handler(exception) || raise

    end
    
    def safe_subresource(subresource, options=Hash.new)
      url = client.concat_urls(client.url, subresource)
      options = client.options.dup.merge! options
      block = client.block
      if block
        client.class.new(url, options, &block)
      else
        client.class.new(url, options)
      end
    end
  end
  puts "SWITCH TO ACTIVERECORD DRIVER"
  Rubydora::Repository.send(:include, ActiveFedora::ActiveRecordDriver)

  ActiveRecord::Base.establish_connection(
    adapter: 'sqlite3',
    database: 'db/development.sqlite3',
    pool: 5,
    timeout: 5000
  )


  conn = ActiveRecord::Base.connection
  
  #conn.drop_table(:datastreams)
  unless conn.table_exists?(:datastreams)
    conn.create_table(:datastreams) do |t|
      t.column :pid, :string
      t.column :dsid, :string
      t.column :label, :string
      t.column :content_type, :string
      t.column :content, :text
      t.column :versionable, :boolean
    end
  end

  unless conn.index_name_exists?(:datastreams, 'datastream_by_pid_and_dsid', nil)
    conn.add_index(:datastreams, [:pid, :dsid], unique: true, name: 'datastream_by_pid_and_dsid')
  end

  #conn.drop_table(:repo_objects)
  unless conn.table_exists?(:repo_objects)
    conn.create_table(:repo_objects) do |t|
      t.column :pid, :string
      t.column :label, :string
      t.column :state, :string
    end
  end
  unless conn.index_name_exists?(:repo_objects, 'repo_objects_by_pid', nil)
    conn.add_index(:repo_objects, :pid, unique: true, name: 'repo_objects_by_pid')
  end
  #conn.drop_table(:sequences)
  unless conn.table_exists?(:sequences)
    conn.create_table(:sequences) do |t|
      t.column :value, :integer, default: 0
    end
  end
end
