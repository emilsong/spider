var ng = require('nodegrass'); 
var spiders = [];

function urlParam(url,name){
	var reg = new RegExp("(^|&)"+ name +"=([^&]*)(&|$)"); 
	var search = url.substr(url.indexOf("?"));
	var r = search.substr(1).match(reg);  
	if (r!=null) return r[2]; return ""; 
}
function clone(obj){  
    var o;  
    switch(typeof obj){  
    case 'undefined': break;  
    case 'string'   : o = obj + '';break;  
    case 'number'   : o = obj - 0;break;  
    case 'boolean'  : o = obj;break;  
    case 'object'   :  
        if(obj === null){  
            o = null;  
        }else{  
            if(obj instanceof Array){  
                o = [];  
                for(var i = 0, len = obj.length; i < len; i++){  
                    o.push(clone(obj[i]));  
                }  
            }else{  
                o = {};  
                for(var k in obj){  
                    o[k] = clone(obj[k]);  
                }  
            }  
        }  
        break;  
    default:          
        o = obj;break;  
    }  
    return o;     
}  

// 对Date的扩展，将 Date 转化为指定格式的String   
// 月(M)、日(d)、小时(h)、分(m)、秒(s)、季度(q) 可以用 1-2 个占位符，   
// 年(y)可以用 1-4 个占位符，毫秒(S)只能用 1 个占位符(是 1-3 位的数字)   
// 例子：   
// (new Date()).Format("yyyy-MM-dd hh:mm:ss.S") ==> 2006-07-02 08:09:04.423   
// (new Date()).Format("yyyy-M-d h:m:s.S")      ==> 2006-7-2 8:9:4.18   
Date.prototype.Format = function(fmt)   
{ 
  var o = {   
    "M+" : this.getMonth()+1,                 //月份   
    "d+" : this.getDate(),                    //日   
    "h+" : this.getHours(),                   //小时   
    "m+" : this.getMinutes(),                 //分   
    "s+" : this.getSeconds(),                 //秒   
    "q+" : Math.floor((this.getMonth()+3)/3), //季度   
    "S"  : this.getMilliseconds()             //毫秒   
  };   
  if(/(y+)/.test(fmt))   
    fmt=fmt.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));   
  for(var k in o)   
    if(new RegExp("("+ k +")").test(fmt))   
  fmt = fmt.replace(RegExp.$1, (RegExp.$1.length==1) ? (o[k]) : (("00"+ o[k]).substr((""+ o[k]).length)));   
  return fmt;   
}  



var com = {send :function(msg){console.log(msg.text);}};



var getRegExec = function(regStr,str,idx,prefix,endfix){
	var idx = idx||1;
	var prefix = prefix||"";
	var endfix = endfix||"";
	var reg = new RegExp(regStr);
	var match = reg.exec(str);
	if(match!=null)
		return prefix+match[idx]+endfix;
	else
		return "";
};

var getRegMatch = function(regStr,str){
	if(regStr==undefined)
		return str;
	var reg = new RegExp(regStr,'g');
	return str.match(reg);
}

var getRegMatchStr = function(regStr,str,idx,prefix,endfix){
	
	if(str==null)
		return "";
	var idx = idx||1;
	var prefix = prefix||"";
	var endfix = endfix||"";
	var reg = new RegExp(regStr,'g');
	
	var matches = str.match(reg);
	
	var re = "";
	if(matches!=null){
		for(var i=0;i<matches.length;i++){
			re += getRegExec(regStr,matches[i],idx,prefix,endfix);
		}
	}
	return re;
}



var processDataClass = function(){
	var def = {};
	var datas = [];
	var processIndex = 0;
	var getProcessData = function (dataProcessDef,dataVal,callback) {

		if(processIndex<dataVal.length){
			var data = dataVal[processIndex];
			var process = new processClass();
			process.init(dataProcessDef.process,data[dataProcessDef.field],function(rdata){

				if(rdata.val.length>0){
					for(p in rdata.val[0]){
						dataVal[processIndex][p] = rdata.val[0][p];
					}
				}
				//console.log(dataVal[processIndex]);

				processIndex++;
				getProcessData(dataProcessDef,dataVal,callback);

			});
		}else{
			//debugger;
			if(callback)
				callback(dataVal);
		}


	};

	this.init = function(dataDef,dataVal,callback){
		
		var def = clone(dataDef);
		
		var dataProcessDef;
		for(var i=0;i<def.length;i++){
			if(def[i].process!=undefined&&def[i].process.length>0){
				if(def[i].processed==undefined){
					dataProcessDef = def[i];
					def[i]["processed"] = 1;
				}
			}
		}
		if(dataProcessDef!=undefined){
			getProcessData(dataProcessDef,dataVal,callback);
		}
		else{
			if(callback)
				callback(dataVal);
		}
	};
};



var processClass = function(){
	
	var pindex = 0;
	var getProcessData = function (process,result,callback) {
		//debugger;
		var datas = [];
		var arr;
		if(toString.apply(result)!='[object Array]'){
			arr = [];
			arr.push(result);
		}else
			arr = result;
		for(var a=0; a<arr.length;a++){
			var match = arr[a];
			var data = {};
			if(process.data!=undefined){

				for(var i=0;i<process.data.length;i++){
					var dataset = process.data[i];
					if(dataset.regOp=='g')
						data[dataset.field] = getRegMatchStr(dataset.regExp,match,dataset.matchIndex,dataset.prefix,dataset.endfix);
					else
						data[dataset.field] = getRegExec(dataset.regExp,match,dataset.matchIndex,dataset.prefix,dataset.endfix);
					
					if(data[dataset.field].length<100)
						com.send({"act":"log","text":"获取数据-"+dataset.field+":"+data[dataset.field]});
				}

			}
			datas.push(data);
		}
		if(callback)
			callback({def:process.data,val:datas});
	};

	var getProcess = function(processes,result,callback){	
		var data = data||{};

		if(pindex<processes.length){
			var process = processes[pindex];
			pindex++;
			
			switch(process.processType){
				case 3:
					var url = result;
					var encode = "gbk";
					if(process.encode!=undefined)
						encode = process.encode;
					if(url!=undefined&&url!=""){
						ng.get(url,function(html,status,headers){
							//console.log(status);
							//console.log(headers);
							//console.log(html);
							com.send({"act":"log","text":"获取内容-"+url});
							var text = html;
							if(process.regExp)
								text= getRegExec(process.regExp,html,process.matchIndex,process.prefix,process.endfix);
							
							if(process.data!=undefined){
								getProcessData(process,text,callback);
							}else{
								getProcess(processes,text,callback);
							}
						},null,encode).on('error', function(e) {
							console.log("Got error: " + e.message);
						});
						
					}else{
						if(process.data!=undefined){
							getProcessData(process,"",callback);
						}else{
							getProcess(processes,"",callback);
						}
					}
				break;
				case 2:
					var matches = getRegMatch(process.regExp,result);
					getProcessData(process,matches,callback);
				break;

			}
		}
	};

	this.init = function(processes,result,callback){
		getProcess(processes,result,callback);
	};

};

var postDataClass = function() {
	var postIndex;
	var post = function(datas,spider,callback){
		if(postIndex===undefined)
			postIndex = (datas.length - 1);
		if(postIndex>=0){
			var data = datas[postIndex];
			if(spider.postData){
				for(var key in spider.postData){
					
					if(spider.postData[key].indexOf("?")!=-1)//判断是否是从URL参数获取数据，格式是“?参数名”
						data[key] = urlParam(spider.homeUrl,spider.postData[key].substr(1));
					else
						data[key] = spider.postData[key];
				}
			}
			var tmp = "";
			var i = 0;
			for(var key in data){
				if(i<2){
					tmp+= " "+data[key];
				}
				i++;
			}
			if(data.title)
				tmp = data.title;
			if(tmp.length>100)
				tmp = tmp.substr(0,100);
			
				
			var querystring = require('querystring');
			var REQ_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded','Content-Length':querystring.stringify(data).length};
			ng.post(spider.postUrl,function(result){
				//console.log(data);
				//console.log(spider.postUrl);
				postIndex--;
				var json = JSON.parse(result);
				if(json.code==0)
					com.send({"act":"log","text":"保存数据成功-"+tmp});
				else
					com.send({"act":"log","text":json.msg+"-"+tmp});
				setTimeout(function(){
					post(datas,spider,callback);
				},1000);
			},REQ_HEADERS,data);
		}else{
			if(callback){
				callback();
			}
		}

	};

	this.init = function(datas,spider,callback){
		if(!spider.postBatch){
			////debugger;
			post(datas,spider,callback);
		}else{
			//var datastr = {data:JSON.stringify(datas)};
			var REQ_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded','Content-Length':querystring.stringify(data).length};
			ng.post(spider.postUrl,function(){
				if(callback){
					callback();
				}
			},REQ_HEADERS,datas);
		}
	};
};


var spiderTaskClass = function(){
	var taskIndex = 0;
	var taskRun = function(spider,callback){
				
		if(spider.spiderTask.urlDynamic!=undefined){
			
			ng.get(spider.spiderTask.urlFrom,function(result,status,headers){
				
				result = JSON.parse(result);
				
				if(result.code==0){
					
					spider.homeUrl = spider.spiderTask.urlDynamic.replace(/\$param1\$/g,result.data.param1);
					spider.spiderTask.urlReport1 = spider.spiderTask.urlReport.replace("$param1$",result.data.param1);
					var processRoot = new processClass();
					processRoot.init(spider.process,spider.homeUrl,function(datas1){
				
						//console.log(datas1);

						var process2 = new processDataClass();
						process2.init(datas1.def,datas1.val,function(datas2){
							//debugger;
							//console.log(datas2);
							
							var post = new postDataClass();
							
							post.init(datas2,spider,function(){
								
								if(spider.spiderTask.urlReport1!=undefined){
					
									ng.get(spider.spiderTask.urlReport1,function(result,status,headers){
										taskIndex++;
										taskRun(spider,callback);
									},null,'utf8').on('error', function(e) {
										console.log("Got error: " + e.message);
									});
										
								}else{
								
									taskIndex++;
									taskRun(spider,callback);
								}

							});

						});

					});
				}else{
					if(callback)
						callback();
				}
				
			},null,'utf8').on('error', function(e) {
				console.log("Got error: " + e.message);
			});
			
		}
		else{
			if(taskIndex<spider.spiderTask.urlList.length){
				
				spider.homeUrl = spider.spiderTask.urlList[taskIndex];
				var processRoot = new processClass();
				processRoot.init(spider.process,spider.homeUrl,function(datas1){
			
					//console.log(datas1);

					var process2 = new processDataClass();
					process2.init(datas1.def,datas1.val,function(datas2){
						//debugger;
						//console.log(datas2);
						
						var post = new postDataClass();
						
						post.init(datas2,spider,function(){
							
							taskIndex++;
							taskRun(spider,callback);

						});

					});

				});
			}else{
				if(callback)
					callback();
			}
		}

	};
	this.init = function(spider,callback){
		if(spider.spiderTask==undefined){
			spider.spiderTask = {
				urlList : []
				
			};
		}
		//处理任务抓取信息列表
		if(spider.homeUrl!=""){
			spider.spiderTask.urlList = [];
			spider.spiderTask.urlList.push(spider.homeUrl);
		}else{
			if(spider.spiderTask.urlList.length==0){
				for(var i=spider.spiderTask.urlBegin;i>=spider.spiderTask.urlEnd;i--){
					spider.spiderTask.urlList.push(spider.spiderTask.urlTemplate.replace('(*)',i));
				}
				console.log(spider.spiderTask.urlList);
			}
		}
		taskRun(spider,callback);
	}
};


var getSpiders = function(url){
	ng.get(url,function(data,status,headers){
		//console.log(status);
		//console.log(headers);
		//console.log(data);
		var spiderObjs = {};
		spiders = [];
		var json = JSON.parse(data);
		if(json.data.list&&json.data.list.length>0){
			
			for(var i=0;i<json.data.list.length;i++){			
				spiderObjs[json.data.list[i].id+""] = json.data.list[i];
			}
			console.log("====get spider define====");
			//console.log(spiderObjs);
			for(var kkk in spiderObjs){
				var spider = spiderObjs[kkk];
				if(spider.spiderid!=0){
					var spiderMeta = JSON.parse(spider.spider);
					//console.log(spider.spiderid);
					var spiderReal = JSON.parse(spiderObjs[spider.spiderid].spider);
					//console.log(spiderReal);
					for(var kk in spiderMeta){
						spiderReal[kk] = spiderMeta[kk];
					}
					spiderReal["id"] = spider.id;
					spiders.push(spiderReal);
				}else{
					var spiderReal = JSON.parse(spider.spider);
					spiderReal["id"] = spider.id;
					spiders.push(spiderReal);
				}
			}
			//console.log(spiders);
			batchCrawl(spiders,url);
			
		}
	},null,'utf8').on('error', function(e) {
		console.log("Got error: " + e.message);
	});
}


exports.start = function(url){
	getSpiders(url);
}

var batchCrawl = function(spiders,url){
	
	var spiderIndex = 0;
	function redo(){
		if(spiderIndex<spiders.length){
			var spider = spiders[spiderIndex];
			console.log("=====start=====");
			console.log(spider.spiderName);
			console.log((new Date()).Format("yyyy-MM-dd hh:mm:ss"));
			spiderIndex++;
			var t = new spiderTaskClass();
			t.init(spider,function(){
				console.log(spider.spiderName);
				console.log("=====end=====");
				console.log((new Date()).Format("yyyy-MM-dd hh:mm:ss"));
				//Settings.setValue(spider.id,(new Date()).Format("yyyy-MM-dd hh:mm:ss"));
				//Settings.setObject("spider",false);
				delete t;
				redo();
			});
			
		}else{
			
			console.log("全部执行完毕/15分钟后继续");
			setTimeout(function(){
				getSpiders(url);
			},900000);
			
			console.log("2小时更新一次采集状态");
			setTimeout(function(){
				
				ng.get("http://cms.dz.renren.com/index.php?api=fund_list_spider&act=reset",function(html,status,headers){
					var json = JSON.parse(html);
					console.log(json);
				},null,'utf8');
				
			},1000*3600*2);
			
		}
	}
	redo();
	
}




