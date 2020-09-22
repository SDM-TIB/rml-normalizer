const parser = require('rocketrml');

const doMapping = async () => {
		const args = process.argv.slice(2);
		//console.log(args[0]);
		const options = {
    toRDF: true,
    verbose: true,
    xmlPerformanceMode: false,
    replace: false,
  };
		console.log(args[0]);
		console.log(args[1]);
  const result = await parser.parseFile(args[0], args[1], options).catch((err) => { console.log(err); });
			//console.log(result);
};

doMapping();
