require_relative 'spec_helper'

compatible_image_flavors.each do | flavor|
  describe "Running Containers with different configurations - #{flavor}" do
    before(:all) do
      @image = find_image(flavor)
    end

    before(:each) {
      @container = start_container(@image, options)
    }

    after(:each) do
      cleanup_container(@container)
    end

    context 'Support single pipeline configured via volume bind' do
      let(:options) { {"HostConfig" => { "Binds" => ["#{FIXTURES_DIR}/simple_pipeline/:/usr/share/logstash/pipeline/"] } } }

      it "be running the supplied single pipeline" do
        expect(get_node_stats['pipelines']['main']['plugins']['inputs'][0]['id']).to eq 'simple_pipeline'
      end
    end

    context 'Support multiple pipelines configured via volume bind' do
      let(:options) { {"HostConfig" => { "Binds" => ["#{FIXTURES_DIR}/multiple_pipelines/pipelines/:/usr/share/logstash/pipeline/",
                                                     "#{FIXTURES_DIR}/multiple_pipelines/config/pipelines.yml:/usr/share/logstash/config/pipelines.yml"] } } }

      it "should be running both supplied pipelines" do
        expect(get_node_stats['pipelines']['pipeline_one']['plugins']['inputs'][0]['id']).to eq 'multi_pipeline1'
        expect(get_node_stats['pipelines']['pipeline_two']['plugins']['inputs'][0]['id']).to eq 'multi_pipeline2'
      end
    end

    context 'Support custom logstash.yml via volume bind' do
      let(:options) { {"HostConfig" => { "Binds" => ["#{FIXTURES_DIR}/custom_logstash_yml/logstash.yml:/usr/share/logstash/config/logstash.yml"] } } }

      it "has changed the value of pipeline.batch.size" do
        expect(get_node_info['pipelines']['main']['batch_size']).to eq 200
      end
    end
  end
end