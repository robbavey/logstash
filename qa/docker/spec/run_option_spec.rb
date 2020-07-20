require_relative 'spec_helper'

compatible_image_flavors.each do | flavor|
  describe "Running Containers, setting options - #{flavor}" do
    before(:all) do
      @image = find_image(flavor)
    end

    before(:each) do
      @container = start_container(@image, { 'ENV' => env})
    end

    after(:each) do
      cleanup_container(@container)
    end

    context 'setting pipeline workers shell style' do
      let(:env) { ['PIPELINE_WORKERS=32'] }

      it "should have 32 pipeline workers set" do
        expect(get_node_info['pipelines']['main']['workers']).to eq 32
      end
    end

    context 'setting pipeline workers dot style' do
      let(:env) { ['pipeline.workers=64'] }

      it "should have 64 pipeline workers set" do
        expect(get_node_info['pipelines']['main']['workers']).to eq 64
      end
    end

    context 'setting pipeline batch size' do
      let(:env) {['pipeline.batch.size=123']}

      it 'should set batch size to 123' do
        expect(get_node_info['pipelines']['main']['batch_size']).to eq 123
      end
    end

    context 'setting pipeline batch delay' do
      let(:env) {['pipeline.batch.delay=36']}

      it 'should set batch delay to 36' do
        expect(get_node_info['pipelines']['main']['batch_delay']).to eq 36
      end
    end

    context 'setting unsafe shutdown to true shell style' do
      let(:env) {['pipeline.unsafe_shutdown=true']}

      it 'should set unsafe shutdown to true' do
        expect(get_settings['pipeline.unsafe_shutdown']).to be_truthy
      end
    end

    context 'setting unsafe shutdown to true dot style' do
      let(:env) {['pipeline.unsafe_shutdown=true']}

      it 'should set unsafe shutdown to true' do
        expect(get_settings['pipeline.unsafe_shutdown']).to be_truthy
      end
    end

    unless is_oss?(flavor)
      context 'disable xpack monitoring' do
        let(:env) {['xpack.monitoring.enabled=false']}

        it 'should set monitoring to false' do
          expect(get_settings['xpack.monitoring.enabled']).to be_falsey
        end
      end

      context 'set elasticsearch urls as an array' do
        let(:env) { ['xpack.monitoring.elasticsearch.password="hithere"', 'xpack.monitoring.elasticsearch.hosts=["http://node1:9200","http://node2:9200"]']}

        it 'should set monitoring to false' do
          expect(get_settings['xpack.monitoring.elasticsearch.hosts']).to be_an(Array)
          expect(get_settings['xpack.monitoring.elasticsearch.hosts']).to include('http://node1:9200')
          expect(get_settings['xpack.monitoring.elasticsearch.hosts']).to include('http://node2:9200')
        end
      end
    end
  end
end
