// From example: https://stackoverflow.com/questions/27740649/how-to-find-all-leaves-of-group-with-javascript-and-json

const schema_json = require('./module1_schema.json');
/**
function getLeafNodes(leafNodes, obj){
    if (obj.fields) {
        obj.fields.forEach(function(field){getLeafNodes(leafNodes,field)});
    } else {
        leafNodes.push(obj);
    }
}

function getName(json) { 
   if (json.fields) {       
        if (json.name != '__key__') {
           var leafNodes = [];   
           getLeafNodes(leafNodes,json);
           console.log(leafNodes.map(function(leafNode){ 
             return leafNode.name; 
           })); //Logs leaf node ids to the console
        } else {
           json.field.forEach( function(field) {                               
              getName(field,name);
           });      
        }   
   }
}

getName(json)
**/
function isArray(obj) {
    return obj.constructor === Object;
}


function isObject(obj) {
    return obj.constructor === Array;
}

// we assume that if it is not another object or array
// it is a primitive type
// obviously not true in javascript but true for this module usage
function isPrimitive(item) {
    return !isObject(item) && !isArray(item);
}

flatten = function (obj) {
    // assert what we got was actually an object
    console.assert(isObject(obj) || isArray(obj));

    var dict = {};
    var dot = '.';

    /**
     **/
    function _flatten(obj, dict, keyPrefix) {

        if (obj === null) {
            return dict;
        }

        // object has -ad minimum - a key, value pair
        // {'some-key': 'some-value-maybe-another-object'}
        // decide if object or array

        for (var name in obj) {

            if (obj.hasOwnProperty(name)) {

                var keyName;
                if (keyPrefix) {
                    keyName = keyPrefix + dot + name;
                } else {
                    keyName = name;
                }

                if (isPrimitive(obj[name])) {
                    dict[keyName] = obj[name];
                } else {
                    // continue recursing
                    // TODO assumes objects, need to deal with arrays too
                    _flatten(obj[name], dict, keyName);

                }

            }

        }

        return dict;

    }

    return _flatten(obj, dict, null);
};
console.log(schema_json)
//console.log(flatten(schema_json))
