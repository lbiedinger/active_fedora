module ActiveFedora
  module Associations
    class RDF < SingularAssociation #:nodoc:
      def replace(values)
        ids = Array(values).reject(&:blank?)
        raise "can't modify frozen #{owner.class}" if owner.frozen?
        destroy
        ids.each do |id|
          uri = ::RDF::URI(ActiveFedora::Base.id_to_uri(id))
          owner.resource.insert [owner.rdf_subject, reflection.predicate, uri]
        end
        owner.send(:attribute_will_change!, reflection.name)
      end

      def reader
        filtered_results.map { |val| ActiveFedora::Base.uri_to_id(val) }
      end

      def destroy
        filtered_results.each do |candidate|
          owner.resource.delete([owner.rdf_subject, reflection.predicate, candidate])
        end
      end

      private

        # @return [Array<RDF::URI>] the rdf results filtered to objects that match the specified class_name consraint
        def filtered_results
          if filtering_required?
            filter_by_class(rdf_uris)
          else
            rdf_uris
          end
        end

        def filtering_required?
          return false if reflection.klass == ActiveFedora::Base
          reflections_with_same_predicate.count > 1
        end

        # Count the number of reflections that have the same predicate as the reflection
        # for this association.
        def reflections_with_same_predicate
          owner.class.outgoing_reflections.select { |_k, v| v.options[:predicate] == reflection.predicate }
        end

        # @return [Array<RDF::URI>]
        def rdf_uris
          rdf_query.map(&:object)
        end

        # @return [Array<RDF::Statement>]
        def rdf_query
          owner.resource.query(subject: owner.rdf_subject, predicate: reflection.predicate).enum_statement
        end

        # @return [Array<RDF::URI>]
        def filter_by_class(candidate_uris)
          return [] if candidate_uris.empty?
          ids = candidate_uris.map { |uri| ActiveFedora::Base.uri_to_id(uri) }
          results = ActiveFedora::SolrService.query(ActiveFedora::SolrQueryBuilder.construct_query_for_ids(ids), rows: 10_000)

          results.select { |result| result.model? reflection.klass }.map(&:rdf_uri)
        end
    end
  end
end
