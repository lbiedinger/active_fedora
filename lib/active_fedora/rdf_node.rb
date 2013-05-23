# Ugly hack to allow defining of properties in the RDF vocabulary's namespace (ie. RDF.type, RDF.value) within map_properties
module RDF
  # This enables RDF to respond_to? :value so you can make assertions with http://www.w3.org/1999/02/22-rdf-syntax-ns#value
  def self.value 
    self[:value]
  end
end

module ActiveFedora
  module RdfNode
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    autoload :TermProxy

    # Mapping from URI to ruby class
    def self.rdf_registry
      @@rdf_registry ||= {}
    end


    ##
    # Get the subject for this rdf object
    def rdf_subject
      @subject ||= begin
        s = self.class.rdf_subject.call(self)
        s &&= RDF::URI.new(s) if s.is_a? String
        s
      end
    end   

    def reset_rdf_subject!
      @subject = nil
    end
    
    # def attributes=(attibutes_hash)
    #   attibutes_hash.each_pair do |property, value|
    #     
    #   end
    # end
    
    # Specifies the default location for writing values on this type of Node.
    # This primarily affects what happens when you use `=` or `<<` to set values on a Node or use `.value` to get the values of a node.
    # To make your Node classes write/read values to/from a custom location, override this method
    # Defaults to using :value property, which defaults to using the RDF.value predicate `http://www.w3.org/1999/02/22-rdf-syntax-ns#value`
    # This method should always return an Array of properties that can be traversed using the current node as the starting point.
    # 
    # Note: Much of the behavior that this setting affects is implemented in the `populate_default` method.
    #
    # @example Use default behavior to put assertions in `http://www.w3.org/1999/02/22-rdf-syntax-ns#value`
    #   @ds.topic = "Cosmology"
    #   @ds.value
    #   => ["Cosmology"]
    #
    # @example Set default write point to [:elementList, :topicElement]
    #   class Topic
    #     include ActiveFedora::RdfObject
    #     def default_write_point_for_values 
    #       [:elementList, :topicElement]
    #     end
    #   
    #     # rdf_type DummyMADS.Topic
    #     map_predicates do |map|
    #       map.elementList(in: DummyMADS, to: "elementList", class_name:"DummyMADS::ElementList")
    #     end
    #   end
    #   class ElementList
    #     include ActiveFedora::RdfObject
    #     rdf_type DummyMADS.elementList
    #     map_predicates do |map|
    #       map.topicElement(in: DummyMADS, to: "TopicElement")
    #     end
    #   end
    #
    #   @ds.topic = "Cosmology"
    #   @ds.topic(0).elementList.topicElement
    #   => "Cosmology"
    #   @ds.topic.value = ["Cosmology"]
    def default_write_point_for_values 
      [:value]
    end

    # @param [RDF::URI] subject the base node to start the search from
    # @param [Symbol] term the term to get the values for
    def get_values(subject, term, *args)
      options = config_for_term_or_uri(term)
      predicate = options[:predicate]
      proxy = TermProxy.new(self, subject, predicate, options)
      if args.first.kind_of?(Integer)
        return proxy.nodeset[args.first]
      else
        return proxy
      end
    end

    def target_class(predicate)
      _, conf = self.class.config_for_predicate(predicate)
      if conf.nil?
        raise "The #{self.class} RDF Class does not have a predicate called #{predicate.inspect}.  Available predicates are: #{self.class.config.values.map {|v| v[:predicate].to_s}}"
      end
      class_name = conf[:class_name]
      return nil unless class_name
      ActiveFedora.class_from_string(class_name, self.class)
    end

    # if there are any existing statements with this predicate, replace them
    # @param [RDF::URI] subject  the subject to insert into the graph
    # @param [Symbol, RDF::URI] predicate  the predicate to insert into the graph
    # @param [Array,#to_s] values  the value/values to insert into the graph
    def set_value(subject, predicate, values)
      options = config_for_term_or_uri(predicate)
      predicate = options[:predicate]
      if values.kind_of? Hash
        remove_existing_values(subject, predicate, values.keys)
        values.each_pair do |pred, val|
          pred_uri = find_predicate(pred) unless pred.kind_of? RDF::URI
          append(subject, pred_uri, val)
        end
      else
        values = Array(values)  
        remove_existing_values(subject, predicate, values)
        values.each do |arg|
          append(subject, predicate, arg)
        end
      end
      
      TermProxy.new(self, subject, predicate, options)
    end
    
    def delete_predicate(subject, predicate, values = nil)
      predicate = find_predicate(predicate) unless predicate.kind_of? RDF::URI

      if values.nil?
        query = RDF::Query.new do
          pattern [subject, predicate, :value]
        end

        query.execute(graph).each do |solution|
          graph.delete [subject, predicate, solution.value]
        end
      else
        Array(values).each do |v|
          graph.delete [subject, predicate, v]
        end
      end
    end

    # append a value
    # @param [Symbol, RDF::URI] predicate  the predicate to insert into the graph
    def append(subject, predicate, value)
      options = config_for_term_or_uri(predicate)
      term_proxy = TermProxy.new(self, subject, predicate, options)

      if predicate == :value
        unless self.class.config.has_key?(:value)
          self.class.map_predicates {|map| map.value(in: RDF)}
        end
      end
      
      if value.respond_to?(:rdf_subject) # an RdfObject
        graph.insert([subject, predicate, value.rdf_subject ])
      elsif value.kind_of? Array  # If it's an array of values, repeat append method for each of the values
        value.each do |val|
          self.append(self.rdf_subject, predicate, val)
        end
      elsif options.has_key?(:class_name) # If a class_name has been associated with the property being set, build a node based on that class & insert the values into that node.  
        new_node = term_proxy.build 
        if value.kind_of? Hash
          value.each_pair do |pred, val|
            pred_uri = new_node.find_predicate(pred)
            new_node.append(new_node.rdf_subject, pred_uri, val)
          end
        else
          new_node.populate_default(value, options)
        end
      else # Everything else converted into string literals.  Note: Hashes are converted to strings if no class_name was available b/c you would need the properties defined in an RDF::Node Class to parse the Hash. 
        value = value.to_s if value.kind_of? RDF::Literal
        graph.insert([subject, predicate, value])
      end
      
      return term_proxy
    end
    
    
    # Returns the (sometimes computed) value of the current node
    def value
      if default_write_point_for_values.first == :value
      
        # This inserts support for RDF.value into any Class that doesn't already have it.
        # Possibly we should make RDF::Object or RDF::Node automatically do this by default? - MZ 05-2013
        unless self.class.config.has_key?(:value)
          self.class.map_predicates {|map| map.value(in: RDF)}
        end
        
        return get_values(self.rdf_subject, :value)
      else
        return retrieve_values(default_write_point_for_values)
      end
    end
    
    # Set values within the node according to its Class defaults
    # If no defaults have been set on the Class, values are inserted as RDF.value properties, accessible on all nodes as .value
    # If you want `.value` to map to somewhere else, simply set the :value property on your Class.
    def populate_default(values, options)
      parent = self
      if default_write_point_for_values.length > 1
        path = default_write_point_for_values.dup
        last_property = path.pop
        parent = retrieve_node(path)
      else
        last_property = default_write_point_for_values.first
      end
      parent.set_value(parent.rdf_subject, last_property, values)
    end
    
    # Retrieves a node based on path composed of node properties to traverse
    def retrieve_node(node_path, options={build_nodes: true})
      build_nodes = options.has_key?(:build_nodes) ? options[:build_nodes] : true
      current_node = self
      node_path.each do |property,i|
        if current_node.send(property).empty?
          if build_nodes
            current_node = current_node.send(property).build
          else
            raise "Could not retrieve node at #{node_path}. There is no node at #{property}.  Would have automatically generated the missing nodes, but you set `:build_nodes` to `false`. Current graph: #{puts graph.dump(:ntriples)}"
          end
        else
          current_node = current_node.send(property).nodeset.first
        end
      end
      return current_node
    end
    
    # Traverse a path of node properties and return the value of the last property
    def retrieve_values(node_path)
      last_property_from_path = node_path.pop
      if node_path.empty?
        node = self
      else
        node = retrieve_node(node_path)
      end
      return node.get_values(node.rdf_subject, last_property_from_path)
    end

    def config_for_term_or_uri(term)
      case term
      when RDF::URI
        self.class.config.each { |k, v| return v if v[:predicate] == term}
      when nil
        {}
      else
        result = self.class.config[term.to_sym]
        if result.nil?
          return {}
        else
          return result
        end
      end
    end

    # @param [Symbol, RDF::URI] term predicate  the predicate to insert into the graph
    def find_predicate(term)
      conf = config_for_term_or_uri(term)
      conf ? conf[:predicate] : nil
    end

    def query subject, predicate, &block
      predicate = find_predicate(predicate) unless predicate.kind_of? RDF::URI
      
      q = RDF::Query.new do
        pattern [subject, predicate, :value]
      end

      q.execute(graph, &block)
    end

    def attributes=(values)
      set_value(rdf_subject, nil, values)
    end
    
    def method_missing(name, *args)
      if (md = /^([^=]+)=$/.match(name.to_s)) && pred = find_predicate(md[1])
          set_value(rdf_subject, pred, *args)  
      elsif find_predicate(name)
          get_values(rdf_subject, name, *args)
      else 
        super
      end
    rescue ActiveFedora::UnregisteredPredicateError
      super
    end

    private

    def remove_existing_values(subject, predicate, values)
      if values.any? { |x| x.respond_to?(:rdf_subject)}
        values.each do |arg|
          if arg.respond_to?(:rdf_subject) # an RdfObject
            # can't just delete_predicate, have to delete the predicate with the class
            values_to_delete = find_values_with_class(subject, predicate, arg.class.rdf_type)
            delete_predicate(subject, predicate, values_to_delete)
          else
            delete_predicate(subject, predicate)
          end
        end
      else
        delete_predicate(subject, predicate)
      end
    end


    def find_values_with_class(subject, predicate, rdf_type)
      matching = []
      query = RDF::Query.new do
        pattern [subject, predicate, :value]
      end
      query.execute(graph).each do |solution|
        if rdf_type
          query2 = RDF::Query.new do
            pattern [solution.value, RDF.type, rdf_type]
          end
          query2.execute(graph).each do |sol2|
            matching << solution.value
          end
        else
          matching << solution.value
        end
      end
      matching 
    end
    class Builder
      def initialize(parent)
        @parent = parent
      end

      def build(&block)
        yield self
      end

      def method_missing(name, *args, &block)
        args = args.first if args.respond_to? :first
        raise "mapping must specify RDF vocabulary as :in argument" unless args.has_key? :in
        vocab = args[:in]
        field = args.fetch(:to, name).to_sym
        class_name = args[:class_name]
        raise "Vocabulary '#{vocab.inspect}' does not define property '#{field.inspect}'" unless vocab.respond_to? field
        indexing = false
        if block_given?
          # needed for solrizer integration
          indexing = true
          iobj = IndexObject.new
          yield iobj
          data_type = iobj.data_type
          behaviors = iobj.behaviors
        end
        @parent.config[name] = {:predicate => vocab.send(field) } 
        # stuff data_type and behaviors in there for to_solr support
        if indexing
          @parent.config[name][:type] = data_type
          @parent.config[name][:behaviors] = behaviors
        end
        @parent.config[name][:class_name] = class_name if class_name
      end

      # this enables a cleaner API for solr integration
      class IndexObject
        attr_accessor :data_type, :behaviors
        def initialize
          @behaviors = []
          @data_type = :string
        end
        def as(*args)
          @behaviors = args
        end
        def type(sym)
          @data_type = sym
        end
        def defaults
          :noop
        end
      end
    end

    module ClassMethods
      def config
        @config ||= {}
      end

      def map_predicates(&block)
        builder = Builder.new(self)
        builder.build &block
      end

      def rdf_type(uri_or_string=nil)
        if uri_or_string
          uri = uri_or_string.kind_of?(RDF::URI) ? uri_or_string : RDF::URI.new(uri_or_string) 
          self.config[:type] = {predicate: RDF.type}
          @rdf_type = uri
          ActiveFedora::RdfNode.rdf_registry[uri] = self
        end
        @rdf_type
      end

      def config_for_predicate(predicate)
        config.each do |term, value|
          return term, value if value[:predicate] == predicate
        end
        return nil
      end

      ##
      # Register a ruby block that evaluates to the subject of the graph
      # By default, the block returns the current object's pid
      # @yield [ds] 'ds' is the datastream instance
      def rdf_subject &block
        if block_given?
           return @subject_block = block
        end

        # Create a B-node if they don't supply the rdf_subject
        @subject_block ||= lambda { |ds| RDF::Node.new }
      end

    end
  end
end

