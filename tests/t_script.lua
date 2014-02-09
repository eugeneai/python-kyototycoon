-- This is a subset of "example/ktscrex.lua" from the Kyoto Tycoon 0.9.56 source distribution.

kt = __kyototycoon__
db = kt.db

-- log the start-up message
if kt.thid == 0 then
   kt.log("system", "loaded Lua script for 'play_script' unit tests")
end

-- echo back the input data as the output data
function echo(inmap, outmap)
   for key, value in pairs(inmap) do
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- store a record
function set(inmap, outmap)
   local key = inmap.key
   local value = inmap.value
   if not key or not value then
      return kt.RVEINVALID
   end
   local xt = inmap.xt
   if not db:set(key, value, xt) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- retrieve the value of a record
function get(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local value, xt = db:get(key)
   if value then
      outmap.value = value
      outmap.xt = xt
   else
      local err = db:error()
      if err:code() == kt.Error.NOREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end
