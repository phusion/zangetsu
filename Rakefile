task :default => ['lib/optapdb/native_support.node']

task :clean do
	sh "rm -rf lib/optapdb/native_support.node ext/build ext/.lock-wscript"
end

file 'lib/optapdb/native_support.node' => 'ext/build/default/native_support.node' do
	sh "ln -sf ../../ext/build/default/native_support.node lib/optapdb/native_support.node"
end

file 'ext/build/default/native_support.node' => ['ext/native_support.cpp', 'ext/build'] do
	sh "cd ext && node-waf build"
end

file 'ext/build' => 'ext/wscript' do
	sh "cd ext && node-waf configure"
end