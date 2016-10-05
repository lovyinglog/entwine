/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <json/json.h>

#include <entwine/types/defs.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/format-packing.hpp>
#include <entwine/types/format-types.hpp>
#include <entwine/types/point-pool.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

// The Format contains the attributes that give insight about what the tree
// looks like at a more micro-oriented level than the Structure, which gives
// information about the overall tree structure.  Whereas the Structure can
// tell us about the chunks that exist in the tree, the Format can tell us
// about what those chunks look like.
class Format
{
public:
    Format(
            const Schema& schema,
            const Delta* delta,
            bool trustHeaders = true,
            bool compress = true,
            HierarchyCompression hierarchyCompression =
                HierarchyCompression::Lzma,
            std::vector<std::string> tailFields = std::vector<std::string> {
                "numPoints", "chunkType"
            },
            std::string srs = std::string());

    Format(const Schema& schema, const Delta* delta, const Json::Value& json);

    Format(const Format& other)
        : m_schema(other.schema())
        , m_delta(maybeClone(other.delta()))
        , m_trustHeaders(other.trustHeaders())
        , m_compress(other.compress())
        , m_hierarchyCompression(other.hierarchyCompression())
        , m_tailFields(other.tailFields())
        , m_srs(other.srs())
    { }

    Format& operator=(const Format& other)
    {
        m_schema = other.schema();
        m_delta = maybeClone(other.delta());
        m_trustHeaders = other.trustHeaders();
        m_compress = other.compress();
        m_hierarchyCompression = other.hierarchyCompression();
        m_tailFields = other.tailFields();
        m_srs = other.srs();

        return *this;
    }

    Json::Value toJson() const
    {
        Json::Value json;
        json["srs"] = m_srs;
        json["trustHeaders"] = m_trustHeaders;
        json["compress"] = m_compress;

        for (const TailField f : m_tailFields)
        {
            json["tail"].append(tailFieldNames.at(f));
        }

        json["compress-hierarchy"] =
            hierarchyCompressionNames.count(m_hierarchyCompression) ?
                hierarchyCompressionNames.at(m_hierarchyCompression) :
                "none";

        return json;
    }

    std::unique_ptr<std::vector<char>> pack(
            Data::PooledStack dataStack,
            ChunkType chunkType) const;

    Unpacker unpack(std::unique_ptr<std::vector<char>> data) const
    {
        return Unpacker(*this, std::move(data));
    }

    const Schema& schema() const { return m_schema; }
    const Delta* delta() const { return m_delta.get(); }
    const TailFields& tailFields() const { return m_tailFields; }

    bool trustHeaders() const { return m_trustHeaders; }
    bool compress() const { return m_compress; }
    const std::string& srs() const { return m_srs; }
    std::string& srs() { return m_srs; }
    HierarchyCompression hierarchyCompression() const
    {
        return m_hierarchyCompression;
    }

private:
    Schema m_schema;
    std::unique_ptr<Delta> m_delta;

    bool m_trustHeaders;
    bool m_compress;
    HierarchyCompression m_hierarchyCompression;
    TailFields m_tailFields;
    std::string m_srs;
};

} // namespace entwine

